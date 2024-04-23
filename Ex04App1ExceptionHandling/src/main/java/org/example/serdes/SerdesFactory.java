package org.example.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.domain.GreetingMessage;

public class SerdesFactory {

    public static Serde<GreetingMessage> greetingMessageSerde(){
        return new GreetingSerde();
    }

    public static Serde<GreetingMessage> greetingSerdesusingGenerics(){
        JsonSerializer<GreetingMessage> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<GreetingMessage> jsonDeserializer = new JsonDeserializer<>(GreetingMessage.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
    }
}
