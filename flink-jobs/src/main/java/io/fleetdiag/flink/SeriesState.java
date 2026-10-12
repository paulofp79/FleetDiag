package io.fleetdiag.flink;

/** Per-series keyed state of the scoring job. Flink POJO: public fields, no-arg constructor. */
public class SeriesState {
    /** end_time of the last sample scored, epoch millis; 0 = none yet */
    public long lastEndTime;

    // EWMA fallback, updated with every sample (outliers clamped)
    public double ewmaMean;
    public double ewmaVar;
    public long ewmaN;

    // last W samples, index 0 = newest. ringEnd = 0 means "not flagged".
    public long[] ringEnd;
    public double[] ringZ;
    public double[] ringObs;
    public double[] ringRef;

    // open anomaly
    public boolean active;
    public long activeStart;
    public long activeEnd;
    public double peakZ;
    public double peakObs;
    public double peakRef;
    public String peakSource;
    public int peakSlot;
    public double peakScale;

    public SeriesState() {}

    public static SeriesState create(int window) {
        SeriesState s = new SeriesState();
        s.ringEnd = new long[window];
        s.ringZ = new double[window];
        s.ringObs = new double[window];
        s.ringRef = new double[window];
        return s;
    }

    void clearRing() {
        java.util.Arrays.fill(ringEnd, 0L);
        java.util.Arrays.fill(ringZ, 0.0);
        java.util.Arrays.fill(ringObs, 0.0);
        java.util.Arrays.fill(ringRef, 0.0);
    }
}
