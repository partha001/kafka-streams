package org.example.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class Ex07GreetingsTopologyWithPeekOperatorTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    /**
     * a topology defines how the data is to be processsed and it consites to 3 things
     * 1. SourceProcessor 2.StreamProcessor and 3.SinkProcessor
     *
     * @return
     */
    public static Topology buildTopoloogy() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetinsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String())); //behind the scene it uses consumer api
        //greetinsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream")); //this line is just for logging/debugging purposes

        var modifiedStream = greetinsStream
                .filter((key, value)-> value.length()>5)
                .peek((key,value)-> log.info("after filter : key:{} value:{}",key,value)) //helps us in inspecting and debugging
                .flatMapValues((key,value) -> {
                    var newValues = Arrays.asList(value.split(""));
                    return newValues
                            .stream()
                            .map(String::toUpperCase)
                            .collect(Collectors.toList());
                })
                .peek((key,value)->  {
                    key.toUpperCase(); //any transformation applied in peek is not reflected. thus to consumer the key appears in smallcase only
                    log.info("after flatMapValues : key:{} value:{}",key,value);
                });

        //modifiedStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream")); //this line is just for logging/debugging purposes

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String())); //behind the scene it uses the producer api

        return streamsBuilder.build();
    }
}
