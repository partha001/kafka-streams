package org.example.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.example.domain.GreetingMessage;

public class GreetingSerde implements Serde<GreetingMessage> {

    //instantiating the mapper here
    private final ObjectMapper mapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,false);


    //finally passing the mapper to the serializer and deserializer

    @Override
    public Serializer<GreetingMessage> serializer() {
        return new GreetingMessageSerializer(mapper);
    }

    @Override
    public Deserializer<GreetingMessage> deserializer() {
        return new GreetingMessageDeserializer(mapper);
    }
}
