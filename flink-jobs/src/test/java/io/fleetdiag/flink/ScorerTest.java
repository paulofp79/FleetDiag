package io.fleetdiag.flink;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ScorerTest {
    // Monday 2026-10-12 10:00:30 UTC -> slot 10
    private static final long T0 = Instant.parse("2026-10-12T10:00:30Z").toEpochMilli();
    private static final long MIN = 60_000L;
    private static final String METRIC = "Redo Generated Per Sec";

    private static Sample sample(int minute, double v) {
        return new Sample("local", "FREE", 1, METRIC, v, T0 + minute * MIN);
    }

    private static Baseline baseline(double med, double mad) {
        Baseline b = new Baseline();
        b.key = "local/FREE/1/" + METRIC;
        b.med = new double[Baseline.SLOTS];
        b.mad = new double[Baseline.SLOTS];
        Arrays.fill(b.med, med);
        Arrays.fill(b.mad, mad);
        b.computedAt = "2026-10-12T00:00:00Z";
        return b;
    }

    private static List<Anomaly> run(Scorer sc, SeriesState s, Baseline b, double... values) {
        List<Anomaly> out = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            Anomaly a = sc.score(s, sample(i, values[i]), b);
            if (a != null) {
                out.add(a);
            }
        }
        return out;
    }

    @Test
    void slotMatchesSparkConvention() {
        assertEquals(0, Scorer.slotOf(Instant.parse("2026-10-12T00:00:00Z").toEpochMilli())); // Monday
        assertEquals(10, Scorer.slotOf(T0));
        assertEquals(167, Scorer.slotOf(Instant.parse("2026-10-11T23:59:59Z").toEpochMilli())); // Sunday
    }

    @Test
    void steadyValuesDoNotFire() {
        Scorer sc = new Scorer(new Scorer.Params());
        SeriesState s = SeriesState.create(5);
        assertTrue(run(sc, s, baseline(2000, 100), 2000, 2100, 1900, 2050, 1950, 2000, 2200).isEmpty());
    }

    @Test
    void threeOfFiveFiresOnceThenUpdatesSameKey() {
        Scorer sc = new Scorer(new Scorer.Params());
        SeriesState s = SeriesState.create(5);
        // scale = max(1.4826*100, 5% of 2000 = 100) = 148.26; 4 sigma = 593
        List<Anomaly> out = run(sc, s, baseline(2000, 100), 2000, 5000, 2000, 5000, 6000, 5000);
        // hits at minutes 1, 3, 4 -> open at minute 4; minute 5 is another hit -> one update
        assertEquals(2, out.size(), "open at the 3rd hit, then one update per further hit");
        String key = out.get(0).anomalyKey;
        assertTrue(out.stream().allMatch(a -> a.anomalyKey.equals(key)));
        assertEquals(40, key.length());
        Anomaly first = out.get(0);
        assertEquals(T0 + 1 * MIN - MIN, first.windowStart, "starts at the first flagged minute");
        assertEquals(T0 + 4 * MIN, first.windowEnd);
        assertEquals("seasonal", first.baselineSource);
        assertEquals("up", first.direction);
        assertEquals(6000, first.observed, 1e-9, "peak taken over the flagged samples in the window");
        Anomaly last = out.get(1);
        assertEquals(T0 + 5 * MIN, last.windowEnd);
        assertEquals(6000, last.observed, 1e-9, "peak observed value");
        assertEquals("CRITICAL", last.severity);
    }

    @Test
    void replayGivesSameKey() {
        double[] v = {2000, 5000, 2000, 5000, 6000};
        String k1 = run(new Scorer(new Scorer.Params()), SeriesState.create(5), baseline(2000, 100), v).get(0).anomalyKey;
        String k2 = run(new Scorer(new Scorer.Params()), SeriesState.create(5), baseline(2000, 100), v).get(0).anomalyKey;
        assertEquals(k1, k2);
    }

    @Test
    void duplicateEndTimeIgnored() {
        Scorer sc = new Scorer(new Scorer.Params());
        SeriesState s = SeriesState.create(5);
        Baseline b = baseline(2000, 100);
        assertNull(sc.score(s, sample(0, 5000), b));
        assertNull(sc.score(s, sample(0, 5000), b)); // same end_time again
        assertNull(sc.score(s, sample(1, 5000), b));
        assertNotNull(sc.score(s, sample(2, 5000), b), "third distinct hit opens");
    }

    @Test
    void flatZeroMetricUsesAbsoluteFloor() {
        // MAD 0 and median 0 (idle database): small noise must not fire
        Scorer sc = new Scorer(new Scorer.Params());
        SeriesState s = SeriesState.create(5);
        assertTrue(run(sc, s, baseline(0, 0), 0, 0.5, 1, 2, 3, 0.2).isEmpty());
        // a real jump does
        assertFalse(run(sc, SeriesState.create(5), baseline(0, 0), 50, 60, 70).isEmpty());
    }

    @Test
    void ewmaFallbackAfterWarmup() {
        Scorer.Params p = new Scorer.Params();
        Scorer sc = new Scorer(p);
        SeriesState s = SeriesState.create(5);
        double[] v = new double[40];
        for (int i = 0; i < 40; i++) {
            v[i] = 2000 + (i % 2 == 0 ? 50 : -50);
        }
        assertTrue(run(sc, s, null, v).isEmpty(), "no baseline and warm-up: nothing fires");
        List<Anomaly> out = new ArrayList<>();
        for (int i = 40; i < 45; i++) {
            Anomaly a = sc.score(s, sample(i, 9000), null);
            if (a != null) {
                out.add(a);
            }
        }
        assertFalse(out.isEmpty());
        assertEquals("ewma", out.get(0).baselineSource);
    }

    @Test
    void quietWindowClosesAndNextAnomalyGetsNewKey() {
        Scorer sc = new Scorer(new Scorer.Params());
        SeriesState s = SeriesState.create(5);
        List<Anomaly> out = run(sc, s, baseline(2000, 100),
                5000, 5000, 5000, 2000, 2000, 2000, 2000, 2000, 5000, 5000, 5000);
        assertEquals(2, out.size());
        assertNotEquals(out.get(0).anomalyKey, out.get(1).anomalyKey);
    }

    @Test
    void longGapResetsWindow() {
        Scorer sc = new Scorer(new Scorer.Params());
        SeriesState s = SeriesState.create(5);
        Baseline b = baseline(2000, 100);
        assertNull(sc.score(s, sample(0, 5000), b));
        assertNull(sc.score(s, sample(1, 5000), b));
        // collector down for 10 minutes: earlier hits no longer count
        assertNull(sc.score(s, sample(12, 5000), b));
        assertNull(sc.score(s, sample(13, 5000), b));
        assertNotNull(sc.score(s, sample(14, 5000), b));
    }
}
