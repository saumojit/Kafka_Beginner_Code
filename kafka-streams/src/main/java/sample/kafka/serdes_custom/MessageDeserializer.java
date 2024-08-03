package sample.kafka.serdes_custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sample.kafka.beans.Message;
import java.io.IOException;



public class MessageDeserializer implements Deserializer<Message> {
    Logger log= LoggerFactory.getLogger(MessageDeserializer.class.getSimpleName());
    private ObjectMapper objectMapper ;

    public MessageDeserializer(ObjectMapper objectMapper){
        this.objectMapper = objectMapper;
    }
    @Override
    public Message deserialize(String s, byte[] bytes) {
        try {
            return objectMapper.readValue(bytes , Message.class);
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
