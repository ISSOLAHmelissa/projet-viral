package fr.esgi.kafka.viral.common;

/**
 * Resultat d'une validation.
 * Porte le message BRUT en plus de l'objet parse : la DLQ exige le message
 * original, il doit donc survivre jusqu'au point de routage.
 *
 * reason == null : message valide, value est renseigne.
 * reason != null : message invalide, raw + reason partent en DLQ.
 */
public record Checked<T>(String raw, T value, String reason) {

    public static <T> Checked<T> valid(String raw, T value) {
        return new Checked<>(raw, value, null);
    }

    public static <T> Checked<T> rejected(String raw, String reason) {
        return new Checked<>(raw, null, reason);
    }

    public boolean isValid() {
        return reason == null;
    }
}
