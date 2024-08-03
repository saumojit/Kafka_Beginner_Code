package sample.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


// consumer graceful shutdown :
// if the program exits abruptly ,
public class ConsumerWithShutdownDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerWithShutdownDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("ConsumerWithShutdownDemo Demo 2");
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

        // getting main thread and initiating another thread for graceful shutdown
        Thread main_thread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected Consumer Shutdown , Lets exit by calling comsumer.wakeup() ...");
                consumer.wakeup();

                // join the main thread to allow the execution of code in main_thread
                try {
                    main_thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        // subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                log.info("Polling ...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key : " + record.key() + " Value : " + record.value() +
                            " Partition-" + record.partition() + " Offset : " + record.offset());
                }
            }
        }
        catch ( WakeupException e){
            log.error("Consumer is about to shutdown");
        }
        catch( Exception e){
            log.error("Something Unexpected Occurred");
        }
        finally {
            consumer.close();
            log.info("Consumer is now gracefully shutdown ");
        }
    }
}
