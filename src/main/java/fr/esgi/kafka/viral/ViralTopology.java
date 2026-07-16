package fr.esgi.kafka.viral;

import fr.esgi.kafka.viral.common.Checked;
import fr.esgi.kafka.viral.common.DlqRecord;
import fr.esgi.kafka.viral.common.JsonSerdes;
import fr.esgi.kafka.viral.common.Validator;
import fr.esgi.kafka.viral.model.Interaction;
import fr.esgi.kafka.viral.model.Post;
import fr.esgi.kafka.viral.model.Trend;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;

/**
 * Topologie du projet VIRAL.
 */
public final class ViralTopology {

    /** Les hashtags sont dans le texte : "Best moment #paris #photo". */
    private static final Pattern HASHTAG = Pattern.compile("#([a-z0-9]+)");

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

        // -----------------------------------------------------------------
        // VIR-2 - Top hashtags
        // -----------------------------------------------------------------
        trendingHashtags(posts);

        // VIR-3 - Detection de post viral (+ enrichissement via table posts)
        //                                                    -> Topics.ALERTS_VIRAL
        // VIR-4 - Engagement par auteur (jointure interactions x posts)
        //                                                    -> Topics.ENGAGEMENT_BY_AUTHOR
        // VIR-5 - Detection de bots                          -> Topics.ALERTS_BOTS
        // VIR-6 (bonus) - Moderation par mots interdits      -> Topics.MODERATION
    }

    /**
     * VIR-2 - Compte les hashtags sur des fenetres glissantes de 10 min qui
     * avancent toutes les minutes (hopping). Un evenement tombe donc dans
     * 10 fenetres a la fois : c'est ce qui donne un "top a la minute" sans
     * attendre la fin d'un bloc de 10 min.
     */
    private static void trendingHashtags(KStream<String, Post> posts) {

        posts
                // 1 post -> N hashtags (0 si le texte n'en contient aucun).
                .flatMapValues(post -> extraireHashtags(post.text()))

                // La cle passe de post_id a hashtag. Kafka Streams doit donc
                // REPARTITIONNER : sans ca, le meme hashtag vivrait sur
                // plusieurs partitions et serait compte plusieurs fois.
                // groupBy() cree pour nous le topic de repartition.
                .groupBy((postId, hashtag) -> hashtag,
                        Grouped.with("hashtags", Serdes.String(), Serdes.String()))

                // Grace de 180 min : le sujet annonce des retards de 30 a
                // 180 min. Avec une grace a 0 on les jetterait, et les
                // fenetres passees seraient fausses.
                .windowedBy(TimeWindows
                        .ofSizeAndGrace(Duration.ofMinutes(10), Duration.ofMinutes(180))
                        .advanceBy(Duration.ofMinutes(1)))

                .count(Materialized.as("trends-counts"))

                .toStream()
                // La cle est un Windowed<String> : on la ramene au hashtag
                // seul (critere du ticket : cle de sortie = hashtag) et on
                // remet les bornes de la fenetre dans la valeur.
                .map((fenetre, compte) -> KeyValue.pair(
                        fenetre.key(),
                        JsonSerdes.toJson(new Trend(
                                fenetre.key(),
                                compte,
                                fenetre.window().startTime().toString(),
                                fenetre.window().endTime().toString()))))

                .to(Topics.TRENDS, Produced.with(Serdes.String(), Serdes.String()));
    }

    private static List<String> extraireHashtags(String texte) {
        List<String> hashtags = new ArrayList<>();
        if (texte == null) {
            return hashtags;
        }
        Matcher m = HASHTAG.matcher(texte.toLowerCase(Locale.ROOT));
        while (m.find()) {
            hashtags.add(m.group(1));
        }
        return hashtags;
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
