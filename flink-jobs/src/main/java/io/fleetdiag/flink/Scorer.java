package io.fleetdiag.flink;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Robust z-score against the seasonal baseline, with an EWMA fallback. No Flink dependency so the
 * rules can be unit tested. See docs/ARCHITECTURE.md, "Detection".
 *
 * <pre>
 *   z     = (x - center) / max(1.4826 * MAD, floor)       seasonal slot present
 *   z     = (x - ewmaMean) / max(ewmaStd, floor)          fallback, after warm-up
 *   floor = max(relFloor * |center|, absFloor(metric))
 *   fire when |z| >= threshold in at least minHits of the last window samples
 * </pre>
 *
 * Starting values to tune against a week of real data.
 */
public class Scorer implements Serializable {

    public static class Params implements Serializable {
        public double threshold = 4.0;
        public double critical = 8.0;
        public int minHits = 3;
        public int window = 5;
        public double relFloor = 0.05;
        public double defaultAbsFloor = 1.0;
        public Map<String, Double> absFloor = new HashMap<>();
        public double ewmaAlpha = 0.1;
        public int ewmaWarmup = 30;
        /** a gap longer than this resets the hit window (collector or database down) */
        public long maxGapMs = 5 * 60_000L;

        public Params() {
            // metrics whose normal values are well below 1
            absFloor.put("Average Active Sessions", 0.5);
            absFloor.put("User Transaction Per Sec", 0.5);
            absFloor.put("Hard Parse Count Per Sec", 0.5);
        }

        double floor(String metric, double center) {
            double abs = absFloor.getOrDefault(metric, defaultAbsFloor);
            return Math.max(relFloor * Math.abs(center), abs);
        }
    }

    private final Params p;

    public Scorer(Params p) {
        this.p = p;
    }

    public Params params() {
        return p;
    }

    /** Hour of week in UTC, 0 = Monday 00:00 ... 167 = Sunday 23:00 (same as spark/baseline.py). */
    public static int slotOf(long epochMillis) {
        ZonedDateTime t = Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC);
        return (t.getDayOfWeek().getValue() - 1) * 24 + t.getHour();
    }

    /**
     * Scores one sample and updates the state. Returns an anomaly to publish (open or update of an
     * open one), or null.
     */
    public Anomaly score(SeriesState s, Sample x, Baseline b) {
        if (x.endTime <= s.lastEndTime) {
            return null; // duplicate or out of order: at-least-once delivery upstream
        }
        if (s.lastEndTime > 0 && x.endTime - s.lastEndTime > p.maxGapMs) {
            s.clearRing();
            s.active = false;
        }
        s.lastEndTime = x.endTime;

        int slot = slotOf(x.endTime);
        double center = Double.NaN;
        double scale = Double.NaN;
        String source = null;
        if (b != null && b.hasSlot(slot)) {
            center = b.med[slot];
            scale = Math.max(1.4826 * b.mad[slot], p.floor(x.metricName, center));
            source = "seasonal";
        } else if (s.ewmaN >= p.ewmaWarmup) {
            center = s.ewmaMean;
            scale = Math.max(Math.sqrt(s.ewmaVar), p.floor(x.metricName, center));
            source = "ewma";
        }

        double z = Double.isNaN(center) ? 0.0 : (x.value - center) / scale;
        boolean flagged = Math.abs(z) >= p.threshold;

        for (int i = p.window - 1; i > 0; i--) {
            s.ringEnd[i] = s.ringEnd[i - 1];
            s.ringZ[i] = s.ringZ[i - 1];
            s.ringObs[i] = s.ringObs[i - 1];
            s.ringRef[i] = s.ringRef[i - 1];
        }
        s.ringEnd[0] = flagged ? x.endTime : 0L;
        s.ringZ[0] = z;
        s.ringObs[0] = x.value;
        s.ringRef[0] = center;

        updateEwma(s, x);

        int hits = 0;
        long firstHit = Long.MAX_VALUE;
        for (int i = 0; i < p.window; i++) {
            if (s.ringEnd[i] != 0L) {
                hits++;
                firstHit = Math.min(firstHit, s.ringEnd[i]);
            }
        }

        if (!s.active) {
            if (hits < p.minHits) {
                return null;
            }
            // open: the window starts at the beginning of the first flagged minute
            s.active = true;
            s.activeStart = firstHit - 60_000L;
            s.activeEnd = x.endTime;
            s.peakZ = 0.0;
            for (int i = 0; i < p.window; i++) {
                if (s.ringEnd[i] != 0L && Math.abs(s.ringZ[i]) > Math.abs(s.peakZ)) {
                    s.peakZ = s.ringZ[i];
                    s.peakObs = s.ringObs[i];
                    s.peakRef = s.ringRef[i];
                }
            }
            s.peakSource = source;
            s.peakSlot = slot;
            s.peakScale = scale;
            return build(s, x, hits);
        }

        if (hits == 0) {
            s.active = false; // quiet for a full window: closed, next one gets a new key
            return null;
        }
        if (!flagged) {
            return null;
        }
        s.activeEnd = x.endTime;
        if (Math.abs(z) > Math.abs(s.peakZ)) {
            s.peakZ = z;
            s.peakObs = x.value;
            s.peakRef = center;
            s.peakSource = source;
            s.peakSlot = slot;
            s.peakScale = scale;
        }
        return build(s, x, hits);
    }

    private void updateEwma(SeriesState s, Sample x) {
        if (s.ewmaN == 0) {
            s.ewmaMean = x.value;
            s.ewmaVar = 0.0;
            s.ewmaN = 1;
            return;
        }
        double v = x.value;
        if (s.ewmaN >= p.ewmaWarmup) {
            // clamp outliers so a spike does not drag the fallback along with it
            double lim = p.threshold * Math.max(Math.sqrt(s.ewmaVar), p.floor(x.metricName, s.ewmaMean));
            v = Math.max(s.ewmaMean - lim, Math.min(s.ewmaMean + lim, v));
        }
        double diff = v - s.ewmaMean;
        s.ewmaMean += p.ewmaAlpha * diff;
        s.ewmaVar = (1 - p.ewmaAlpha) * (s.ewmaVar + p.ewmaAlpha * diff * diff);
        s.ewmaN++;
    }

    private Anomaly build(SeriesState s, Sample x, int hits) {
        Anomaly a = new Anomaly();
        a.fleet = x.fleet;
        a.dbUniqueName = x.dbUniqueName;
        a.instId = x.instId;
        a.metricName = x.metricName;
        a.windowStart = s.activeStart;
        a.windowEnd = s.activeEnd;
        a.observed = s.peakObs;
        a.baseline = s.peakRef;
        a.score = Math.abs(s.peakZ);
        a.severity = a.score >= p.critical ? "CRITICAL" : "WARNING";
        a.baselineSource = s.peakSource;
        a.slot = s.peakSlot;
        a.scale = s.peakScale;
        a.direction = s.peakZ >= 0 ? "up" : "down";
        a.hits = hits;
        a.window = p.window;
        a.anomalyKey = anomalyKey(x.seriesKey(), s.activeStart);
        return a;
    }

    /** Deterministic: same series and start gives the same key, so replays upsert, not duplicate. */
    static String anomalyKey(String seriesKey, long windowStart) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] h = md.digest(("METRIC_DEVIATION|" + seriesKey + "|" + windowStart)
                    .getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(40);
            for (byte v : h) {
                sb.append(String.format("%02x", v));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
