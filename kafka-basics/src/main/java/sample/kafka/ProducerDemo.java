package sample.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Producer Demo 1");
        // create producer properties
        Properties properties = new Properties();

        // adding property for broker connection
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // adding property for serializer
        properties.setProperty( "key.serializer", StringSerializer.class.getName() );
        properties.setProperty("value.serializer", StringSerializer.class.getName() );

        // create producer
        KafkaProducer < String , String > producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord <String , String> record = new ProducerRecord<>("demo_java" , "helloworld");

        // send data -- asynchronous operation
        producer.send(record);

        // flush and close the producer -- synchronous operation

        // flush sends all the data and waits until done
        producer.flush();

        // close inherently also calls flush() , then closes the program
        producer.close();
    }
}
