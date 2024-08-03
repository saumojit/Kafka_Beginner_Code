package sample.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class SimpleWindowsExample {
    private static final Properties properties;
    static final String inputTopic = "input_topic";
    static final String outputTopic = "output_windowed_topic";

    // Kafka-Streams Properties
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // v1 -- for string concatenation
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // v2 -- for numbers's sum
//        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
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

    // concatenate all the strings based on every 2-minute window and flushes after window closes
    static void createConCatStream(final StreamsBuilder builder) {
        final KStream<Void, String> input = builder.stream(inputTopic);
        Duration duration = Duration.ofMinutes(2);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(duration);

        // Strategy ==> 1. Rekey 2. GroupBy 3.WindowBy 4.BusinessLogic 5.Flush After Window Closes
        final KTable<Windowed<String>, String> window_aggr = input
                .selectKey((k ,v)-> "1")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .reduce((v1 , v2) -> v1 + "_" + v2)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()));

        // Convert Windowed<String> keys to String keys
        KStream<String, String> resultStream = window_aggr
                .toStream()
                .map((windowedKey, val) -> {
                    // Time Conversion : UTC To IST
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
                                                                    .withZone(ZoneId.of("UTC+05:30"));
                    String start_time = formatter.format(windowedKey.window().startTime());
                    String end_time = formatter.format(windowedKey.window().endTime());
                    String newKey = " [" + start_time + " - " + end_time + "]";
                    return KeyValue.pair(newKey, val);
                });

        // resultStream.foreach((key, value) -> System.out.println("Key and Value => " +  key + " : "+ value));

        resultStream.to(outputTopic);
    }
}

// Producer - Input will be String
// Streams  - Concatenates all inputs during the window-frame and flushes after window closes
// Consumer - Example : [2024-07-24T13:18:00 - 2024-07-24T13:20:00]    ok1ok1

// Run The Commands
// Producer =>  kafka-console-producer.sh --topic input_topic --bootstrap-server localhost:9092
// Streams =>   ./gradlew :kafka-streams:run -Dclass='SimpleWindowsExample'
// Consumer =>  kafka-console-consumer.sh --topic output_windowed_topic --bootstrap-server localhost:9092 --property print.key=true --from-beginning
