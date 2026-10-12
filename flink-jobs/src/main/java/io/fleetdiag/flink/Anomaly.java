package io.fleetdiag.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;

/** One METRIC_DEVIATION anomaly for ora.anomaly.v1 (schema anomaly.v1). Flink POJO. */
public class Anomaly {
    private static final ObjectMapper JSON = new ObjectMapper();

    public String anomalyKey;
    public String fleet;
    public String dbUniqueName;
    public int instId;
    public String metricName;
    public long windowStart;
    public long windowEnd;
    public double observed;
    public double baseline;
    /** peak |z| over the anomaly so far */
    public double score;
    public String severity;
    public String baselineSource;
    public int slot;
    public double scale;
    public String direction;
    public int hits;
    public int window;

    public Anomaly() {}

    public String kafkaKey() {
        return fleet + "/" + dbUniqueName + "/" + instId;
    }

    public String toJson() {
        ObjectNode n = JSON.createObjectNode();
        n.put("schema", "anomaly.v1");
        n.put("anomaly_key", anomalyKey);
        n.put("fleet", fleet);
        n.put("db_unique_name", dbUniqueName);
        n.put("inst_id", instId);
        n.put("kind", "METRIC_DEVIATION");
        n.put("subject", metricName);
        n.put("window_start", Instant.ofEpochMilli(windowStart).toString());
        n.put("window_end", Instant.ofEpochMilli(windowEnd).toString());
        n.put("observed_val", observed);
        n.put("baseline_val", baseline);
        n.put("score", round(score));
        n.put("severity", severity);
        ObjectNode e = n.putObject("evidence");
        e.put("baseline_source", baselineSource);
        e.put("slot", slot);
        e.put("scale", round(scale));
        e.put("direction", direction);
        e.put("hits", hits);
        e.put("window", window);
        return n.toString();
    }

    private static double round(double v) {
        return Math.round(v * 1000.0) / 1000.0;
    }
}
