package io.fleetdiag.flink;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/** One 60 s metric value from ora.metric.raw.v1 (schema metric_sample.v1). Flink POJO. */
public class Sample {
    public String fleet;
    public String dbUniqueName;
    public int instId;
    public String metricName;
    public double value;
    /** end_time of the Oracle metric interval, epoch millis UTC */
    public long endTime;

    public Sample() {}

    public Sample(String fleet, String dbUniqueName, int instId, String metricName, double value, long endTime) {
        this.fleet = fleet;
        this.dbUniqueName = dbUniqueName;
        this.instId = instId;
        this.metricName = metricName;
        this.value = value;
        this.endTime = endTime;
    }

    /** fleet/db_unique_name/inst_id/metric_name, same identity as the ora.baseline.v1 key */
    public String seriesKey() {
        return fleet + "/" + dbUniqueName + "/" + instId + "/" + metricName;
    }

    public static Sample fromJson(JsonNode n) {
        return new Sample(
                n.get("fleet").asText(),
                n.get("db_unique_name").asText(),
                n.get("inst_id").asInt(),
                n.get("metric_name").asText(),
                n.get("value").asDouble(),
                Instant.parse(n.get("end_time").asText()).toEpochMilli());
    }
}
