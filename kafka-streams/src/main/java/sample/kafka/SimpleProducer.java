package sample.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

// RUN => ./gradlew :kafka-streams:run -Dclass='SimpleProducer'
public class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class.getSimpleName());
    public static void main(String[] args) {

        log.info("Producer With Callback Demo 2");
        // create producer properties
        Properties properties = new Properties();

        // adding property for broker connection
        properties.setProperty("bootstrap.servers","localhost:9092");

        // adding property for serializer
        properties.setProperty("key.serializer", IntegerSerializer.class.getName() );
        properties.setProperty("value.serializer", IntegerSerializer.class.getName() );

        // create producer
        KafkaProducer< Integer , Integer > producer = new KafkaProducer<>(properties);

        int j=0;
        // j-loop here to send the same key twice to the topic
        while(j<1){
            // i-loop produces 10 messages -- asynchronous operation
            log.info("Iteration-" + j);
            for(int i=0 ; i<10 ; i++){
                String topic = "numbers_topic" ;
                Integer key = i ;
                Integer value = i ;
                // create a producer record
                ProducerRecord<Integer , Integer> record = new ProducerRecord<>(topic, key , value);
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
