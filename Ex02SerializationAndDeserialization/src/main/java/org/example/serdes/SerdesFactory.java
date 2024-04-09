package org.example.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.example.domain.GreetingMessage;

public class SerdesFactory {

    public static Serde<GreetingMessage> greetingMessageSerde(){
        return new GreetingSerde();
    }
}
