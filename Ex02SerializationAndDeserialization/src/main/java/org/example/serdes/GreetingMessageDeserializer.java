package org.example.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.example.domain.GreetingMessage;

import java.io.IOException;

@Slf4j
public class GreetingMessageDeserializer implements Deserializer<GreetingMessage> {

    private ObjectMapper mapper;

    public GreetingMessageDeserializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public GreetingMessage deserialize(String topic, byte[] data) {
        try {
            return mapper.readValue(data, GreetingMessage.class);
        } catch (IOException e) {
            log.error("IOException :{}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            log.error("Exception :{}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
