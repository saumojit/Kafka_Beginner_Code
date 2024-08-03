package sample.kafka.serdes_custom_generic;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class JsonDeserializer<T> implements Deserializer<T> {
    Logger log= LoggerFactory.getLogger(JsonDeserializer.class.getSimpleName());
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS , false);
    private Class<T> type ;
    public JsonDeserializer(Class<T> type){
        this.type=type;
    }

    @Override
    public T deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes , this.type);
        } catch (IOException e) {
            log.error("error occured : " , e.getMessage());
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            log.error("error occured :  " , e.getMessage());
            throw new RuntimeException(e);
        }
    }

}
