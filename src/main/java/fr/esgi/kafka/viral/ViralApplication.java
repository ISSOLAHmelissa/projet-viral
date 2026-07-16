package fr.esgi.kafka.viral;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * Point d'entree (fourni). La logique metier se construit dans
 * {@link ViralTopology}. Lancement :
 *
 *   GROUPE=grp07 KAFKA_BOOTSTRAP=localhost:29092 mvn compile exec:java
 */
public final class ViralApplication {

    private ViralApplication() {
    }

    public static void main(String[] args) throws InterruptedException {
        String bootstrap = System.getenv()
                .getOrDefault("KAFKA_BOOTSTRAP", "localhost:29092");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "viral-" + Topics.GROUPE);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Bonus fiabilite : processing.guarantee=exactly_once_v2 (cf. README)

        StreamsBuilder builder = new StreamsBuilder();
        ViralTopology.build(builder);
        Topology topology = builder.build();
        System.out.println(topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        streams.start();
        latch.await();
    }
}
