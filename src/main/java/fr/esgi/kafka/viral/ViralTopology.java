package fr.esgi.kafka.viral;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Construisez ici la topologie du projet VIRAL.
 * Chaque ticket du backlog (README) correspond a un bloc ci-dessous.
 * Rien n'est code pour vous : les commentaires rappellent l'objectif
 * et les APIs candidates.
 */
public final class ViralTopology {

    private ViralTopology() {
    }

    public static void build(StreamsBuilder builder) {

        KStream<String, String> rawInteractions = builder.stream(
                Topics.INTERACTIONS,
                Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> rawPosts = builder.stream(
                Topics.POSTS,
                Consumed.with(Serdes.String(), Serdes.String()));

        // Sanity check de demarrage : verifiez la connexion au cluster,
        // puis SUPPRIMEZ ce peek (il pollue les logs et coute cher).
        rawInteractions.peek((key, value) ->
                System.out.println("[viral] " + key + " -> " + value));

        // -----------------------------------------------------------------
        // VIR-1 - Ingestion fiable (les DEUX flux)
        //   Parser (model.Post / model.Interaction), valider, router les
        //   invalides vers Topics.DLQ avec le message original + raison.
        //   Pistes : split()/branch(), JsonSerdes.parseOrNull(...).
        // -----------------------------------------------------------------

        // VIR-2 - Top hashtags (flatMap + fenetres hopping)  -> Topics.TRENDS
        // VIR-3 - Detection de post viral (+ enrichissement via table posts)
        //                                                    -> Topics.ALERTS_VIRAL
        // VIR-4 - Engagement par auteur (jointure interactions x posts)
        //                                                    -> Topics.ENGAGEMENT_BY_AUTHOR
        // VIR-5 - Detection de bots                          -> Topics.ALERTS_BOTS
        // VIR-6 (bonus) - Moderation par mots interdits      -> Topics.MODERATION
    }
}
