package io.fleetdiag.flink;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Seasonal baseline of one series from ora.baseline.v1 (schema baseline.v1). Flink POJO.
 * 168 slots, hour of week in UTC, 0 = Monday 00:00. A slot without enough samples has NaN median.
 */
public class Baseline {
    public static final int SLOTS = 168;

    public String key;
    public double[] med;
    public double[] mad;
    public int weeksUsed;
    public String computedAt;

    public Baseline() {}

    public boolean hasSlot(int slot) {
        return med != null && !Double.isNaN(med[slot]);
    }

    public static Baseline fromJson(JsonNode n) {
        Baseline b = new Baseline();
        b.key = n.get("fleet").asText() + "/" + n.get("db_unique_name").asText() + "/"
                + n.get("inst_id").asInt() + "/" + n.get("metric_name").asText();
        b.weeksUsed = n.path("weeks_used").asInt(0);
        b.computedAt = n.path("computed_at").asText("");
        b.med = new double[SLOTS];
        b.mad = new double[SLOTS];
        JsonNode slots = n.get("slots");
        for (int i = 0; i < SLOTS; i++) {
            JsonNode s = slots == null ? null : slots.get(i);
            if (s == null || s.isNull()) {
                b.med[i] = Double.NaN;
                b.mad[i] = Double.NaN;
            } else {
                b.med[i] = s.get("med").asDouble();
                b.mad[i] = s.get("mad").asDouble();
            }
        }
        return b;
    }
}
