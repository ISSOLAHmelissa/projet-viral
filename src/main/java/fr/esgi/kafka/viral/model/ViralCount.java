package fr.esgi.kafka.viral.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Etape intermediaire de VIR-3 : le compte d'un post sur une fenetre, avant
 * d'etre enrichi par le referentiel des posts.
 */
public record ViralCount(
        @JsonProperty("post_id") String postId,
        @JsonProperty("count") long count,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd) {
}
