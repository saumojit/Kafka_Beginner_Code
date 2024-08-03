package sample.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;


// Producer - Input will be Strings In Form of Jsons ... Example: {"msg":"hello" , "bot": true }
// Streams  - Counts No of Bots and NonBots
// Consumer - Example :
// bot     {"bot":1}
// non-bot {"non-bot":1}


// Run The Commands
// Producer =>  kafka-console-producer.sh --topic json_streams --bootstrap-server localhost:9092
// Streams =>   ./gradlew :kafka-streams:run -Dclass='SerdeJsonExample'
// Consumer =>  kafka-console-consumer.sh --topic json_streams_upper --bootstrap-server localhost:9092 --property print.key=true --from-beginning


public class SerdeJsonExample {
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
        // Receives InputStream from topic => example "hello"
        final KStream<String, String> jsonevents = builder.stream(inputTopic);

        final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        final String BOT_COUNT_STORE = "bot_count";

        final KStream<String , Long>  outputStream= jsonevents
                                            .mapValues(changeJson -> {
                                                try {
                                                    final JsonNode jsonNode = OBJECT_MAPPER.readTree(changeJson);
                                                    if (jsonNode.get("bot").asBoolean()) {
                                                        // System.out.println("bot");
                                                        return "bot";
                                                    }
                                                    // System.out.println("non-bot");
                                                    return "non-bot";
                                                } catch (IOException e) {
                                                    return "parse-error";
                                                }
                                            })
                                            .groupBy((key, botOrNot) -> botOrNot)
                                            .count().toStream();



        // outputStream.foreach((key, value) -> System.out.println("Key and Value => " +  key + " : "+ value));

        outputStream.mapValues((key, value) -> {
                    final Map<String, Long> kvMap = Map.of(String.valueOf(key), value);
                    try {
                        return OBJECT_MAPPER.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                    })
                .to(outputTopic);
    }
}
