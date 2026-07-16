package fr.esgi.kafka.viral.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Une ligne de tendance : un hashtag et son compte sur une fenetre. */
public record Trend(
        @JsonProperty("hashtag") String hashtag,
        @JsonProperty("count") long count,
        @JsonProperty("window_start") String windowStart,
        @JsonProperty("window_end") String windowEnd) {
}
