package sample.kafka.serdes_custom_generic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonSerializer<T> implements Serializer<T> {
    Logger log= LoggerFactory.getLogger(JsonSerializer.class.getSimpleName());
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS , false);

    @Override
    public byte[] serialize(String s, T json_val ) throws RuntimeException {
        if(json_val == null){
            return null;
        }
        try {
            return this.objectMapper.writeValueAsBytes(json_val);
        } catch (JsonProcessingException e) {
            log.error("error occured " , e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("error occured  " , e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
