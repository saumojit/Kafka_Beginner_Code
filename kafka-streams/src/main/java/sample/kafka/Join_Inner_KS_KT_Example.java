package sample.kafka;

import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import java.util.*;
import java.util.concurrent.ExecutionException;

import sample.kafka.beans.JoinedRecord;
import sample.kafka.serdes_custom_generic.CustomSerdes;

public class Join_Inner_KS_KT_Example {
    private static final Properties properties;
    static final String topic1 =  "topic1";
    static final String topic2 =  "topic2";
    static final String joined_topic_12 = "joined_topic_12";
    static final List<String> topic_name_list = new ArrayList<>(Arrays.asList(topic1 , topic2 , joined_topic_12));

    // Kafka-Streams Properties
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "json_uppercase_app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG , "3000");
        // parallelism -- (optional)
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");

        // exception handlers -- (optional)
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG
                , LogAndContinueExceptionHandler.class);
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
        List<NewTopic> topicList = new ArrayList<NewTopic>();
        Map<String, String> configs = new HashMap<String, String>();
        int partitions = 1;
        Short replication = 1;
        for (String topic : topic_name_list) {
            boolean contains = names.contains(topic);
            if (!contains) {
                newTopic = new NewTopic(topic, partitions, replication).configs(configs);
                topicList.add(newTopic);
            }
        }
        if(topicList.size()>0) {
            admin.createTopics(topicList);
            log.info("Topic Created : " + topicList.get(0) + " and " + topicList.get(1));
        }

        // Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();
        processorFunctionLogic(builder);

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

    static void processorFunctionLogic(final StreamsBuilder builder) {
        final KStream<String, String> stream1 = builder.stream(topic1);
        stream1.peek((key, value) -> System.out.println("I1-put Key and Value Input => " +  key + " : "+ value));
        final KTable<String, String> table2 = builder.table(topic2 , Materialized.as("stream2"));
        table2.toStream().peek((key, value) -> System.out.println("I2-put Key and Value Input => " +  key + " : "+ value));
        ValueJoiner<String , String , JoinedRecord> valueJoiner = JoinedRecord::new;
        final KStream<String, JoinedRecord> stream_joined = stream1.join(table2 , valueJoiner).peek((key, value) -> System.out.println("O-put Key and Value Input => " +  key + " : "+ value));
        stream_joined.to(joined_topic_12 , Produced.with(Serdes.String() , CustomSerdes.joinedRecordSerdes()));
    }
}

// HOW TO :  Run The Commands
// Producer1 =>  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic1 --property parse.key=true --property key.separator=:
// Producer1 Input ==> A : Apple
// Producer2 =>  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic2 --property parse.key=true --property key.separator=:
// Producer2 Input ==> A : It is a vowel
// Streams =>   ./gradlew :kafka-streams:run -Dclass='Join_Inner_KS_KT_Example'
// Consumer =>  kafka-console-consumer.sh --topic joined_topic_12 --bootstrap-server localhost:9092 --property print.key=true --from-beginning
// Consumer Output ==> A       {"val1":" ALL","val2":" It is a vowel"}


// About : ############ Following Code : transforms Objects Values to Uppercase  ################

