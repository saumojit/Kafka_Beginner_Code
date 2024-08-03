package sample.kafka;

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
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import java.util.*;
import java.util.concurrent.ExecutionException;

import sample.kafka.beans.Message;
import sample.kafka.exceptions_custom.StreamsDeserializationExceptionCustom;
import sample.kafka.serdes_custom_generic.CustomSerdes;
public class MessageUCustomSerdeExample {
    private static final Properties properties;
    static final String inputTopic =  "json_streams";
    static final String outputTopic = "json_streams_upper";

    // Kafka-Streams Properties
    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "json_uppercase_app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

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
        boolean contains1 = names.contains(inputTopic);
        boolean contains2 = names.contains(outputTopic);
        List<NewTopic> topicList = new ArrayList<NewTopic>();
        Map<String, String> configs = new HashMap<String, String>();
        int partitions = 4;  // keeping same as kafka threads to enable effective parallelism
        Short replication = 1;
        if (!contains1) {
            newTopic = new NewTopic(inputTopic, partitions, replication).configs(configs);
            topicList.add(newTopic);
        }
        if (!contains2) {
            newTopic = new NewTopic(outputTopic, partitions, replication).configs(configs);
            topicList.add(newTopic);
        }
        if(topicList.size()>0) {
            admin.createTopics(topicList);
            log.info("Topic Created : " + topicList.get(0) + " and " + topicList.get(1));
        }

        // Define the processing topology of tHe Streams application.
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
        final KStream<String, Message> jsonevents = builder.stream(inputTopic ,
                                        Consumed.with(Serdes.String() , CustomSerdes.messageSerdes()));
        final KStream<String , Message>  outputStream= jsonevents
                                            .peek((key, value) -> System.out.println("I-put Key and Value Input => " +  key + " : "+ value))
                                            .mapValues(jsonNode -> {
                                               jsonNode.setMessage(jsonNode.getMessage().toUpperCase());
                                               return jsonNode;
                                            });
        //outputStream.foreach((key, value) -> System.out.println("Key and Value => " +  key + " : "+ value));
        outputStream.to(outputTopic , Produced.with(Serdes.String() , CustomSerdes.messageSerdes()));
    }
}

// HOW TO :  Run The Commands
// Producer =>  kafka-console-producer.sh --topic json_streams --bootstrap-server localhost:9092
// Producer Input ==> {"message":"except masteminds" , "bot": true }
// Streams =>   ./gradlew :kafka-streams:run -Dclass='MessageUCustomSerdeExample'
// Consumer =>  kafka-console-consumer.sh --topic json_streams_upper --bootstrap-server localhost:9092 --property print.key=true --from-beginning
// Consumer Output ==> {"message":"TO CONSUMER1","bot":true,"timestamp":null}


// About : ############ Following Code : transforms Objects Values to Uppercase  ################

// 1 . -- Custom Serde --
// There are two custom serde packages
// 1.1. serdes_custom_generic (Reusable for Other Classes )
// 1.2. serdes_custom ( Custom Serdes only for Message Not Reusable )

// 2. -- Parallelism -- with multiple threads for multiple tasks assigned each assigned to a partition
// Example1 -- Here We Have 4 Threads for 4 Partitions : Each Thread Contains One Task Assigned to A Partition
// Example2 -- Here We Have 2 Threads for 4 Partitions : Each Thread Contains Two Task Assigned to Two Partition Respectively
// Example3 -- Here We Have 1 Threads for 4 Partitions : Each Thread Contains Four Task Assigned to Four Partition Respectively
// available_threads=Runtime.getRuntime().availableProcessors();

// 3. -- Exception Handling --
// DeSerialization --> Processing Topology --> Serialization
// Process will not stop if data mismatch found in deserialization but continue to next data
// LogAndContinueExceptionHandler ==> Process will not stop if error occurs but continue to next data
// StreamsDeserializationExceptionCustom ==> It allows up to 3 Errors per task/thread else it stops the program
// StreamsUncaughtException Handler (TODO)

// Exception Configs
// default.deserialization.exception.handler = class sample.kafka.exceptions_custom.StreamsDeserializationExceptionCustom
// default.production.exception.handler = class org.apache.kafka.streams.errors.LogAndContinueExceptionHandler