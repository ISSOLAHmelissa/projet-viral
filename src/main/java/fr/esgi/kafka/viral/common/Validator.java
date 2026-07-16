package fr.esgi.kafka.viral.common;

import fr.esgi.kafka.viral.model.Interaction;
import fr.esgi.kafka.viral.model.Post;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Set;

/**
 * Validation metier VIR-1.
 *
 * Les records Post et Interaction n'ont que des champs String : Jackson ne
 * plante donc quasiment jamais au parse ("timestamp": "hier a 15h" se
 * deserialise tres bien). Tout le travail est ici.
 *
 * Un evenement en RETARD ou un DOUBLON reste VALIDE : leur JSON est correct.
 * Les ecarter ici ferait perdre des messages valides (critere du socle).
 */
public final class Validator {

    private static final Set<String> TYPES = Set.of("VIEW", "LIKE", "COMMENT", "SHARE");
    private static final Set<String> LANGS = Set.of("fr", "en", "es");

    private Validator() {
    }

    public static Checked<Post> checkPost(String raw) {
        if (raw == null || raw.isBlank()) {
            return Checked.rejected(raw, "empty_message");
        }
        Post post = JsonSerdes.parseOrNull(raw, Post.class);
        if (post == null) {
            return Checked.rejected(raw, "unparsable_json");
        }
        if (isBlank(post.postId())) {
            return Checked.rejected(raw, "missing_field:post_id");
        }
        if (isBlank(post.userId())) {
            return Checked.rejected(raw, "missing_field:user_id");
        }
        // text peut etre vide (photo sans legende) mais pas absent.
        if (post.text() == null) {
            return Checked.rejected(raw, "missing_field:text");
        }
        if (isBlank(post.lang())) {
            return Checked.rejected(raw, "missing_field:lang");
        }
        if (!LANGS.contains(post.lang())) {
            return Checked.rejected(raw, "invalid_enum:lang");
        }
        if (isBlank(post.timestamp())) {
            return Checked.rejected(raw, "missing_field:timestamp");
        }
        if (!isIsoTimestamp(post.timestamp())) {
            return Checked.rejected(raw, "invalid_timestamp");
        }
        return Checked.valid(raw, post);
    }

    public static Checked<Interaction> checkInteraction(String raw) {
        if (raw == null || raw.isBlank()) {
            return Checked.rejected(raw, "empty_message");
        }
        Interaction interaction = JsonSerdes.parseOrNull(raw, Interaction.class);
        if (interaction == null) {
            return Checked.rejected(raw, "unparsable_json");
        }
        if (isBlank(interaction.interactionId())) {
            return Checked.rejected(raw, "missing_field:interaction_id");
        }
        if (isBlank(interaction.postId())) {
            return Checked.rejected(raw, "missing_field:post_id");
        }
        if (isBlank(interaction.userId())) {
            return Checked.rejected(raw, "missing_field:user_id");
        }
        if (isBlank(interaction.type())) {
            return Checked.rejected(raw, "missing_field:type");
        }
        if (!TYPES.contains(interaction.type())) {
            return Checked.rejected(raw, "invalid_enum:type");
        }
        if (isBlank(interaction.timestamp())) {
            return Checked.rejected(raw, "missing_field:timestamp");
        }
        if (!isIsoTimestamp(interaction.timestamp())) {
            return Checked.rejected(raw, "invalid_timestamp");
        }
        return Checked.valid(raw, interaction);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static boolean isIsoTimestamp(String value) {
        try {
            Instant.parse(value);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}
