package fr.esgi.kafka.viral.common;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Message de dead letter queue : raison de rejet + message original.
 * reason est un CODE stable (ex: "missing_field:post_id") et non une phrase :
 * le correcteur compte les raisons distinctes.
 */
public record DlqRecord(
        @JsonProperty("reason") String reason,
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("original") String original) {
}
