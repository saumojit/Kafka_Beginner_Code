package sample.kafka.serdes_custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import sample.kafka.beans.Message;


public class MessageSerde implements Serde<Message> {
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS , false);


    @Override
    public Serializer<Message> serializer() {
        return new MessageSerializer(objectMapper);
    }

    @Override
    public Deserializer<Message> deserializer() {
        return new MessageDeserializer(objectMapper);
    }
}
