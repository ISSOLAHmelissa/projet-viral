package fr.esgi.kafka.viral;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Squelette de tests unitaires de topologie (bonus "tests").
 * Outil : org.apache.kafka.streams.TopologyTestDriver (scope test).
 */
class ViralTopologyTest {

    @Test
    @Disabled("A implementer : un message invalide doit finir dans la DLQ")
    void messageInvalideDoitPartirEnDlq() {
    }

    @Test
    @Disabled("A implementer : 30 LIKE du meme user en 1 min = alerte bot")
    void rafaleDeLikesDeclencheUneAlerteBot() {
    }
}
