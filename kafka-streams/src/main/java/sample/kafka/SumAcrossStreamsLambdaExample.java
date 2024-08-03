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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

// Run The Commands
// Producer =>  Run SimpleProducer Java App ( Only ConsoleProducer Only Work With String Values )
// Streams =>   ./gradlew :kafka-streams:run -Dclass='SumAcrossStreamsLambdaExample'
// Consumer =>  kafka-console-consumer.sh --topic sum_of_odd_numbers_topic --bootstrap-server localhost:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer --from-beginning

public class SumAcrossStreamsLambdaExample {
    private static final Properties properties;
    static final String inputTopic = "numbers_topic";
    static final String outputTopic = "sum_of_odd_numbers_topic";

    // Kafka-Streams Properties
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // v1 -- for string concatenation
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // v2 -- for numbers's sum
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
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
        Logger log= LoggerFactory.getLogger(SumAcrossStreamsLambdaExample.class.getSimpleName());

        // Define the processing topology of tHe Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        // v1 -- concats the string in the stream
        createConCatStream(builder);

        // v2 -- adds the numbers in the stream
        createNumSumStream(builder);

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

    // concatenate strings
    // this works okay with console-producer
    static void createConCatStream(final StreamsBuilder builder) {
        final KStream<Void, String> input = builder.stream(inputTopic);

        final KTable<String, String> sumOfOddNumbers = input
                // odd numbers filteration
                .filter((k, v) -> v.length()==3)
                // we must re-key all records to do groupby key and hence, find out the sum
                .selectKey((k, v) -> "1")
                // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupByKey()
                // Add the numbers to compute the sum.
                .reduce((v1, v2) -> v1 + " _ " + v2);

        sumOfOddNumbers.toStream().to(outputTopic);
    }

    // adds the numbers
    // issue console-producer only passes string serializes not as other type such as int
    // hence , need to build producer with java application
    static void createNumSumStream(final StreamsBuilder builder) {
        final KStream<Integer, Integer> input = builder.stream(inputTopic);
        final KTable<Integer, Integer> sumOfOddNumbers = input
                // even numbers filteration
                .filter((k, v) -> v % 2 == 0)
                // we must re-key all records to do groupby key and hence, find out the sum
                .selectKey((k, v) -> 1)
                // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupByKey()
                // Add the numbers to compute the sum.
                .reduce((v1, v2) -> v1 + v2);
        sumOfOddNumbers.toStream().to(outputTopic);
    }
}