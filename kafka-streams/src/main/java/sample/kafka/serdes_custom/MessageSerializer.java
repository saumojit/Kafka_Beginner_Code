package sample.kafka.serdes_custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sample.kafka.beans.Message;


public class MessageSerializer implements Serializer<Message> {
    Logger log= LoggerFactory.getLogger(MessageSerializer.class.getSimpleName());
    private ObjectMapper objectMapper;

    public MessageSerializer(ObjectMapper objectMapper){
        this.objectMapper=objectMapper;
    }

    @Override
    public byte[] serialize(String s, Message message) throws RuntimeException {
        try {
            return this.objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            log.error("error occured " , e.getMessage());
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("error occured  " , e.getMessage());
            throw new RuntimeException(e);
        }
    }
}
