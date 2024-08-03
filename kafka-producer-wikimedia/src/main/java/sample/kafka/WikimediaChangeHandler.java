package sample.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    String topic ;
    KafkaProducer<String , String> kafkaProducer ;
    // slf4j
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String , String> kafkaProducer, String topic ){
        this.kafkaProducer=kafkaProducer;
        this.topic=topic;
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed()  {
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent) throws Exception {
        // create a producer record
        log.info("Message Sent" + messageEvent.getData());
        ProducerRecord<String , String> record = new ProducerRecord<>(topic , messageEvent.getData());

        // send data -- asynchronous operation
        kafkaProducer.send(record);
    }

    @Override
    public void onComment(String s) throws Exception {

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Message Could Not Be Received , There are some issues");
    }
}
