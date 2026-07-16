package fr.esgi.kafka.viral.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Interaction (topic viral.interactions, cle = post_id). */
public record Interaction(
        @JsonProperty("interaction_id") String interactionId,
        @JsonProperty("post_id") String postId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("type") String type,
        @JsonProperty("timestamp") String timestamp) {
}
