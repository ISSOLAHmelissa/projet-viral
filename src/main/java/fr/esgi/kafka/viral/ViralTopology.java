package fr.esgi.kafka.viral;

import fr.esgi.kafka.viral.common.Checked;
import fr.esgi.kafka.viral.common.DlqRecord;
import fr.esgi.kafka.viral.common.JsonSerdes;
import fr.esgi.kafka.viral.common.Validator;
import fr.esgi.kafka.viral.model.Interaction;
import fr.esgi.kafka.viral.model.Post;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Topologie du projet VIRAL.
 */
public final class ViralTopology {

    private ViralTopology() {
    }

    public static void build(StreamsBuilder builder) {

        // Lecture en String/String et NON avec un serde JSON : un serde JSON
        // planterait a la deserialisation sur un message tronque, avant meme
        // que notre code ne le voie -> thread mort, redemarrage sur le meme
        // offset, boucle infinie (poison pill). En String, aucun octet ne peut
        // faire echouer la lecture : c'est notre code qui decide.
        KStream<String, String> rawInteractions = builder.stream(
                Topics.INTERACTIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawPosts = builder.stream(
                Topics.POSTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // -----------------------------------------------------------------
        // VIR-1 - Ingestion fiable (les DEUX flux)
        // -----------------------------------------------------------------
        KStream<String, Post> posts =
                ingest(rawPosts, Topics.POSTS, Validator::checkPost, "posts");

        KStream<String, Interaction> interactions =
                ingest(rawInteractions, Topics.INTERACTIONS, Validator::checkInteraction, "interactions");

        // VIR-2 - Top hashtags (flatMap + fenetres hopping)  -> Topics.TRENDS
        // VIR-3 - Detection de post viral (+ enrichissement via table posts)
        //                                                    -> Topics.ALERTS_VIRAL
        // VIR-4 - Engagement par auteur (jointure interactions x posts)
        //                                                    -> Topics.ENGAGEMENT_BY_AUTHOR
        // VIR-5 - Detection de bots                          -> Topics.ALERTS_BOTS
        // VIR-6 (bonus) - Moderation par mots interdits      -> Topics.MODERATION
    }

    /**
     * Valide un flux brut : les messages valides ressortent parses, les
     * invalides partent en DLQ avec leur message original et la raison.
     */
    private static <T> KStream<String, T> ingest(
            KStream<String, String> raw,
            String sourceTopic,
            ValueMapper<String, Checked<T>> validator,
            String name) {

        KStream<String, Checked<T>> checked = raw.mapValues(validator);

        Map<String, KStream<String, Checked<T>>> branches = checked
                .split(Named.as(name + "-"))
                .branch((key, value) -> value.isValid(), Branched.as("valid"))
                .defaultBranch(Branched.as("invalid"));

        branches.get(name + "-invalid")
                .mapValues(value -> JsonSerdes.toJson(
                        new DlqRecord(value.reason(), sourceTopic, value.raw())))
                .to(Topics.DLQ, Produced.with(Serdes.String(), Serdes.String()));

        return branches.get(name + "-valid").mapValues(Checked::value);
    }
}
