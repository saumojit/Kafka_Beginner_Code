package sample.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;


// command => ./gradlew :kafka-streams:run -Dclass='StringUpperConvertExample'
public class StringUpperConvertExample {
    private static final Properties properties;

    static final String inputTopic = "streams-plain-input";
    static final String outputTopic = "streams-upper-output";

    // Kafka-Streams Properties
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public static void main(final String[] args) throws ExecutionException, InterruptedException {
        Logger log= LoggerFactory.getLogger(StringUpperConvertExample.class.getSimpleName());

        // Topic creation if it doesn't exist
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);
        ListTopicsResult listTopics = admin.listTopics();
        Set<String> names = listTopics.names().get();
        NewTopic newTopic;
        boolean contains1 = names.contains(inputTopic);
        boolean contains2 = names.contains(outputTopic);
        if (!contains1 && !contains2) {
            List<NewTopic> topicList = new ArrayList<NewTopic>();
            Map<String, String> configs = new HashMap<String, String>();
            int partitions = 1;
            Short replication = 1;
            newTopic = new NewTopic(inputTopic, partitions, replication).configs(configs);
            topicList.add(newTopic);
            newTopic = new NewTopic(outputTopic, partitions, replication).configs(configs);
            topicList.add(newTopic);
            admin.createTopics(topicList);
            log.info("Topic Created : " + topicList.get(0) + " and " + topicList.get(1));
        }

        // Define the processing topology of tHe Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        createToUpperCaseStream(builder);

        // Create Processing Topology
        final Topology appTopology = builder.build();
        log.info("Topology: {}", appTopology.describe());

        // Create KafkaStreams Object
        final KafkaStreams streams = new KafkaStreams(appTopology, properties);

        // Always (and unconditionally) clean local state prior to starting the processing topology.
        streams.cleanUp();

        // Now run the processing topology via `start()` to begin processing its input data.
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static void createToUpperCaseStream(final StreamsBuilder builder) {
        // Receives InputStream from topic => example "hello"
        final KStream<byte[], String> textLines = builder.stream(inputTopic , Consumed.with(Serdes.ByteArray() , Serdes.String()));

        // mapValues ( 1 to 1 Mapping )
        final KStream<byte[] , String> textLinesU = textLines.mapValues(value -> value.toUpperCase());

        // Write the `KStream<Byte[], String>` to the output topic => example "HELLO"
        textLinesU.to(outputTopic , Produced.with(Serdes.ByteArray() , Serdes.String()));
    }
}
