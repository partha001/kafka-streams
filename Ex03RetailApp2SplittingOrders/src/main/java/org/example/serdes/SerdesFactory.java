package org.example.serdes;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.domain.Order;

public class SerdesFactory {



//    public static Serde<GreetingMessage> greetingSerdesusingGenerics(){
//        JsonSerializer<GreetingMessage> jsonSerializer = new JsonSerializer<>();
//        JsonDeserializer<GreetingMessage> jsonDeserializer = new JsonDeserializer<>(GreetingMessage.class);
//        return Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
//    }

    public static Serde<Order> orderSerde(){
        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Order> jsonDeserializer = new JsonDeserializer<>(Order.class);
        return Serdes.serdeFrom(jsonSerializer,jsonDeserializer);
    }
}
