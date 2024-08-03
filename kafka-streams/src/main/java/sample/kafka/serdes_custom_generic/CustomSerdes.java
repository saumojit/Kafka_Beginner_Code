package sample.kafka.serdes_custom_generic;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import sample.kafka.beans.Message;
import sample.kafka.beans.JoinedRecord;


public class CustomSerdes {
    static public Serde<Message> messageSerdes() {
        JsonSerializer<Message>  jsonSerializer= new JsonSerializer<>();
        JsonDeserializer<Message>  jsonDeserializer= new JsonDeserializer<>(Message.class);
        return Serdes.serdeFrom(jsonSerializer , jsonDeserializer);
    }

    static public Serde<JoinedRecord> joinedRecordSerdes() {
        JsonSerializer<JoinedRecord>  jsonSerializer= new JsonSerializer<>();
        JsonDeserializer<JoinedRecord>  jsonDeserializer= new JsonDeserializer<>(JoinedRecord.class);
        return Serdes.serdeFrom(jsonSerializer , jsonDeserializer);
    }
}
