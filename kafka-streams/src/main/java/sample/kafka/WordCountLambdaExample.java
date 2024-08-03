package sample.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serdes;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

// Run The Commands
// Producer =>  kafka-console-producer.sh --topic streams-plaintext-input --bootstrap-server localhost:9092
// Streams =>   ./gradlew :kafka-streams:run -Dclass='WordCountLambdaExample'
// Consumer =>  kafka-console-consumer.sh --topic streams-wordcount-output --bootstrap-server localhost:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

public class WordCountLambdaExample {
    private static final Properties properties;

    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-wordcount-output";

    // Kafka-Streams Properties
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    // Kafka-Topic Creation (If It doesnot exist ) with Properties
    static{
        // Topic creation if it doesn't exist
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(config);
        ListTopicsResult listTopics = admin.listTopics();
        Set<String> names = null;
        try {
            names = listTopics.names().get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
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
            System.out.println("Topic Created : " + topicList.get(0) + " and " + topicList.get(1));
        }
        else{
            System.out.println(String.format("Input Topic %s - and Output Topic - %s Exists In Kafka" , inputTopic , outputTopic));
        }
    }

    public static void main(final String[] args) {
        Logger log= LoggerFactory.getLogger(WordCountLambdaExample.class.getSimpleName());

        // Define the processing topology of tHe Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);

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

    static void createWordCountStream(final StreamsBuilder builder) {
        // Receives InputStream from topic => example "hello hello hello"
        final KStream<String, String> textLines = builder.stream(inputTopic);
        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        // Saves the data to KTable
        final KTable<String, Long> wordCounts = textLines
                // flatMapValues ( 1 to M ) ( Processes Splitted Words against one sentence )
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((keyIgnored, word) -> word)
                .count();

        // Write the `KTable<String, Long>` to the output topic => example "hello" : 3
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
