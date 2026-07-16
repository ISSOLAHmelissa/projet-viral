package fr.esgi.kafka.viral.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Publication (topic viral.posts, cle = post_id).
 * Les hashtags sont DANS le texte (ex: "Best moment #paris #photo") :
 * a vous de les extraire.
 */
public record Post(
        @JsonProperty("post_id") String postId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("text") String text,
        @JsonProperty("lang") String lang,
        @JsonProperty("timestamp") String timestamp) {
}
