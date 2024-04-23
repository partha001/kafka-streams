package org.example.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import org.example.domain.GreetingMessage;

@Slf4j
public class GreetingMessageSerializer implements Serializer<GreetingMessage> {

    private ObjectMapper mapper;

    public GreetingMessageSerializer(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public byte[] serialize(String topic, GreetingMessage data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException : {}",e.getMessage(),e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception : {}",e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }
}
