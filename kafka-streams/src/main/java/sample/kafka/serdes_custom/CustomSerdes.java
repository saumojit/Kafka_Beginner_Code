package sample.kafka.serdes_custom;

import org.apache.kafka.common.serialization.Serde;
import sample.kafka.beans.Message;


public class CustomSerdes{
    static public Serde<Message> messageSerdes() {
        return new MessageSerde();
    }
}
