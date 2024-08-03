package sample.kafka;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

// command : ./gradlew :kafka-consumer-opensearch:run -Dclass='OpenSearchConsumerBulk'
public class OpenSearchConsumerBulk {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));
            System.out.println("Connection to OpenSearch = success");
        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static KafkaConsumer<String , String> createKafkaConsumer(){
        // consumer group name
        String consumer_grp="consumer-opensearch-1";

        Properties properties = new Properties();
        // adding property for broker connection
        properties.setProperty("bootstrap.servers","localhost:9092");
        // adding more properties for consumer config
        properties.setProperty( "key.deserializer", StringDeserializer.class.getName() );
        properties.setProperty( "value.deserializer", StringDeserializer.class.getName() );
        properties.setProperty( "group.id" , consumer_grp);
        properties.setProperty( "auto.offset.reset" , "latest"); // earliest / latest
        // adding property to perform turn auto-commit off
        properties.setProperty( "enable.auto.commit" , "false");

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    public static void main(String[] args) throws IOException  {
        // logger
        Logger log= LoggerFactory.getLogger(OpenSearchConsumerBulk.class.getSimpleName());

        // create an opensearch client
        RestHighLevelClient openSearchClient= createOpenSearchClient();

        // create an index if it doesnot exists
        String index_name="wikimedia";
        boolean ifExists=openSearchClient.indices().exists(new GetIndexRequest(index_name) ,RequestOptions.DEFAULT);
        if(!ifExists){
            CreateIndexRequest indexRequest = new CreateIndexRequest(index_name);
            openSearchClient.indices().create(indexRequest , RequestOptions.DEFAULT);
            log.info("Index Creation as " + index_name + " = success ");
        }
        else{
            log.info("Index couldn't be created as " + index_name + " already exists ");
        }

        // create consumer
        KafkaConsumer<String , String> consumer=createKafkaConsumer();

        // (+) graceful shutdown : getting main thread and initiating another thread
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

        // subscribe
        consumer.subscribe(Arrays.asList("wikimedia.changes"));

        try {
            // actual consumer logic
            while (true) {
                log.info("Polling ...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                int recordCount = records.count();
                log.info("Record Count " + recordCount);
                BulkRequest bulkRequest = new BulkRequest();


                // records batch
                for (ConsumerRecord<String, String> record : records) {
                    //                log.info(record.value());

                    // consumer idempotence -- no impact with duplicate objects
                    // strategy-1 : getting the id from the kafka topic partition
                    String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                    try {
                        // strategy-2 : getting the id from wikimedia data itself
                        id = JsonParser.parseString(record.value()).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();

                        IndexRequest indexRequest = new IndexRequest(index_name).source(record.value(), XContentType.JSON).id(id);
                        //                    IndexResponse response = openSearchClient.index(indexRequest , RequestOptions.DEFAULT);
                        //                    log.info("Inserted One Document To Index = Success : " + response.getId());
                        bulkRequest.add(indexRequest);
                    } catch (OpenSearchStatusException e) {
                        log.error(e.getMessage());
                        log.info("Inserted One Document To Index = Failed : ");
                    }
                }
                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted in bulk mode - " + bulkResponse.getItems().length + " records ");
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                // performing manual commit after all date in a batch gets loaded to consumer
                consumer.commitSync();
                log.info("Batch Committed with " + recordCount + " records");
            }
        }
        // (+) graceful shutdown - catch block
        catch ( WakeupException e){
            log.error("Consumer is about to shutdown");
        }
        catch( Exception e){
            log.error("Something Unexpected Occurred");
        }
        finally {
            consumer.close();
//            openSearchClient.close();
            log.info("Consumer and OpenSearchClient is now gracefully shutdown ");
        }
    }
}
