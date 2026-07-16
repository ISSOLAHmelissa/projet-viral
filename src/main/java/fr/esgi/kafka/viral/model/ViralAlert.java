package fr.esgi.kafka.viral.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Alerte VIR-3 : un post a depasse le seuil, enrichi par son auteur et son texte. */
public record ViralAlert(
        @JsonProperty("post_id") String postId,
        @JsonProperty("author") String author,
        @JsonProperty("text") String text,
        @JsonProperty("interactions") long interactions,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd) {
}
