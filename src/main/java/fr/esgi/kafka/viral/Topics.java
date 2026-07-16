package fr.esgi.kafka.viral;

/**
 * Noms des topics du projet.
 * Entrees fournies par l'enseignant (lecture seule) ; sorties prefixees
 * par votre code groupe (variable d'environnement GROUPE, ex: grp07).
 */
public final class Topics {

    private Topics() {
    }

    // --- Entrees (fournies, ne pas ecrire dedans) ---
    public static final String POSTS = "viral.posts";
    public static final String INTERACTIONS = "viral.interactions";

    /** Code groupe, obligatoire sur le cluster partage. */
    public static final String GROUPE =
            System.getenv().getOrDefault("GROUPE", "grp00");

    public static String out(String suffix) {
        return GROUPE + "." + suffix;
    }

    /**
     * Referentiel interne : les posts VALIDES, republies dans notre espace de
     * noms. Necessaire car Kafka Streams interdit de lire viral.posts a la
     * fois en KStream (VIR-1/VIR-2) et en GlobalKTable (VIR-3).
     */
    public static final String POSTS_REF = out("viral.posts.ref");

    // --- Sorties (a produire par votre application) ---
    public static final String DLQ = out("viral.dlq");
    public static final String TRENDS = out("viral.trends");
    public static final String ALERTS_VIRAL = out("viral.alerts.viral");
    public static final String ENGAGEMENT_BY_AUTHOR = out("viral.engagement.by-author");
    public static final String ALERTS_BOTS = out("viral.alerts.bots");
    public static final String MODERATION = out("viral.moderation");
}
