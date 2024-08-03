package sample.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// if you open multiple consumers within same consumer group
// it will re-balance to the topic
public class ProducerWithCallbackDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerWithCallbackDemo.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Producer With Callback Demo 2");
        // create producer properties
        Properties properties = new Properties();

        // adding property for broker connection
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        // adding property for serializer
        properties.setProperty( "key.serializer", StringSerializer.class.getName() );
        properties.setProperty("value.serializer", StringSerializer.class.getName() );

        // create producer
        KafkaProducer < String , String > producer = new KafkaProducer<>(properties);



        // produces 10 messages -- asynchronous operation
        for(int i=0 ; i<10 ; i++){
            // create a producer record
            ProducerRecord <String , String> record = new ProducerRecord<>("demo_java" , "hello_world_with_callback" + i);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception==null){
                        log.info("Received new metadata : \n" +
                                "topic as : " + metadata.topic() + "\n" +
                                "partition as : " + metadata.partition() + "\n" +
                                "offset as : " + metadata.offset() + "\n" +
                                "timestamp as : " + metadata.timestamp() + "\n"
                        );
                    }
                    else{
                        log.info("Error occurred while sending the data to topic : " + exception);
                    }
                }
            });
        }



        // flush and close the producer -- synchronous operation

        // flush sends all the data and waits until done
        producer.flush();

        // close inherently also calls flush() , then closes the program
        producer.close();
    }
}
