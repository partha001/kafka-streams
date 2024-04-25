package org.example.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.example.domain.GreetingMessage;

import java.time.LocalDateTime;
import java.util.List;

import static org.example.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class Ex06GreetingMockDataProducer {

    static String GREETINGS = "greetings";
    static String GREETINGS_SPANISH = "greetings_spanish";


    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        englishGreetings(objectMapper);
        //spanishGreetings(objectMapper);

    }

    private static void englishGreetings(ObjectMapper objectMapper) {
        var englishGreetings = List.of(
                new GreetingMessage("Hello, Good Morning!", LocalDateTime.now()),
                new GreetingMessage("Hello, Good Evening!", LocalDateTime.now()),
                new GreetingMessage("Transient Error", LocalDateTime.now())
        );
        englishGreetings
                .forEach(greeting -> {
                    try {
                        var greetingJSON = objectMapper.writeValueAsString(greeting);
                        var recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                        log.info("Published the alphabet message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

//    private static void spanishGreetings(ObjectMapper objectMapper) {
//        var spanishGreetings = List.of(
//                new GreetingMessage("¡Hola buenos dias!", LocalDateTime.now()),
//                new GreetingMessage("¡Hola buenas tardes!", LocalDateTime.now()),
//                new GreetingMessage("¡Hola, buenas noches!", LocalDateTime.now())
//        );
//        spanishGreetings
//                .forEach(greeting -> {
//                    try {
//                        var greetingJSON = objectMapper.writeValueAsString(greeting);
//                        var recordMetaData = publishMessageSync(GREETINGS_SPANISH, null, greetingJSON);
//                        log.info("Published the alphabet message : {} ", recordMetaData);
//                    } catch (JsonProcessingException e) {
//                        throw new RuntimeException(e);
//                    }
//                });
//    }

}
