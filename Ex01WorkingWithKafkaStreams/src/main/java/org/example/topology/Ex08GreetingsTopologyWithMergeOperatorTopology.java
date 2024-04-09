package org.example.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class Ex08GreetingsTopologyWithMergeOperatorTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";


    public static Topology buildTopoloogy() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetinsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String())); //behind the scene it uses consumer api

        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), Serdes.String())); //behind the scene it uses consumer api

        var mergedGreetingsStream = greetinsStream.merge(greetingsSpanishStream);

        var modifiedStream =   mergedGreetingsStream
                .mapValues((readOnlyKey,value)-> value.toUpperCase());

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String())); //behind the scene it uses the producer api

        return streamsBuilder.build();
    }
}
