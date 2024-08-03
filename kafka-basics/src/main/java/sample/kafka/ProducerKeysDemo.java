package sample.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// same key will move to same partition on next iteration ( irrespective of broker )
public class ProducerKeysDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerKeysDemo.class.getSimpleName());
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

        int j=0;
        // j-loop here to send the same key twice to the topic
        while(j<2){
            // i-loop produces 10 messages -- asynchronous operation
            log.info("Iteration-" + j);
            for(int i=0 ; i<30 ; i++){
                String topic = "demo_java" ;
                String key = "id_" + i ;
                String value = "HelloWorld_" + i ;
                // create a producer record
                ProducerRecord <String , String> record = new ProducerRecord<>(topic, key , value);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if(exception==null){
                            log.info("Assigned Key " + key + " to Partition-" + metadata.partition());
                        }
                        else{
                            log.info("Error occurred while sending the data to topic : " + exception);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            }
            catch(InterruptedException e){
                e.printStackTrace();
            }
            j++;
        }

        // flush and close the producer -- synchronous operation

        // flush sends all the data and waits until done
        producer.flush();

        // close inherently also calls flush() , then closes the program
        producer.close();
    }
}
