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

import java.util.*;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ExecutionException;

public class ActivityCountPerWindowExample {
    private static final Properties properties;
    static final String inputTopic = "all_actvity_inpt";
    static final String outputTopic = "actv_count_window";

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

    // counts all the activities(strings) based on every 2-minute window and flushes after window closes
    static void createConCatStream(final StreamsBuilder builder) {
        final KStream<Void, String> input = builder.stream(inputTopic);
        Duration duration = Duration.ofMinutes(2);
        TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(duration);

        // Strategy ==> 1. Rekey 2. GroupBy 3.WindowBy 4.BusinessLogic
        // (optional) 5.Convert K,V to String Type (If Needed) 6.Flush After Window Closes
        final KTable<Windowed<String>, String> window_aggr = input
                .selectKey((k ,v)-> "1")
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(tumblingWindow)
                .count()
                .mapValues(v -> "actv_count : " + v.toString())
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


// // //  Windows
// Diff Types - Tumbling , Hopping , Sliding

// -- Tumbling Window --
// It's a simplified window scheme , will be created based on normal clck

// -- Hopping Window --
// A Series of Tumbling Window will be created
// Then it will advance by HOP_time and create another Series of Tumbling Window
// And so on ...
// the windows may overlap.

// -- Sliding --
// Window will only be created based on a record and it may overlap.
// Example -- Say we have a 5-minute window
// A record created at 5:56 pm IST , so the window will be 5:50 pm - 5:56 pm
// A record created at 5:17 pm IST , so the window will be 5:11 pm - 5:17 pm

// // //  I/0
// Producer - Inputs will be String as Activities
// Streams  - <Tumbling Window> Counts Total Activities during the window-frame and flushes after window closes
// Consumer - Example :  [2024-07-24T15:12:00 - 2024-07-24T15:14:00]    actv_count : 6

// // //  Run The Commands
// Producer =>  kafka-console-producer.sh --topic all_actvity_inpt --bootstrap-server localhost:9092
// Streams =>   ./gradlew :kafka-streams:run -Dclass='ActivityCountPerWindowExample'
// Consumer =>  kafka-console-consumer.sh --topic actv_count_window --bootstrap-server localhost:9092 --property print.key=true --from-beginning