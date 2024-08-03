package sample.kafka;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

// command : ./gradlew :kafka-producer-wikimedia:run -Dclass='WikimediaChangesProducer'
public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrap_servers="localhost:9092";

        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG , bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName());

        // set safe-producer i.e. Idempotent producer
        // this block only required for kafka <=3 or 2.8
        // since we have kafka 3.0 , commenting the block
        // properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG , "true");
        // properties.setProperty(ProducerConfig.ACKS_CONFIG , "all");
        // properties.setProperty(ProducerConfig.RETRIES_CONFIG , Integer.toString(Integer.MAX_VALUE));

        // set high throughput producer settings ( Optional but recommended to add )
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG , Integer.toString(1024 * 32)); // 32KB
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG , "20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG , "snappy");

        KafkaProducer<String , String> producer = new KafkaProducer<>(properties);

        // event handler with okhttp and okhttp-eventsource
        EventHandler eventhandler = new WikimediaChangeHandler(producer , "wikimedia.changes");
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventhandler , URI.create(url));
        EventSource eventsource=builder.build();

        eventsource.start();

        TimeUnit.MINUTES.sleep(5);
    }
}
