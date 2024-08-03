package sample.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Consumer Demo 1");
        // create consumer properties
        Properties properties = new Properties();
        String topic = "demo_java";
        String consumer_grp="consumer-grp-1";

        // adding property for broker connection
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // adding more properties for consumer config
        properties.setProperty( "key.deserializer", StringDeserializer.class.getName() );
        properties.setProperty( "value.deserializer", StringDeserializer.class.getName() );
        properties.setProperty( "group.id" , consumer_grp);
        properties.setProperty( "auto.offset.reset" , "earliest");

        // create consumer
        KafkaConsumer< String , String > consumer = new KafkaConsumer<>(properties);

        // subscribe to topic
        consumer.subscribe(Arrays.asList(topic));


        while(true) {
            log.info("Polling ...");
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Key : " + record.key() + " Value : " + record.value() +
                        " Partition-" + record.partition() + " Offset : " + record.offset());
            }
        }
    }
}
