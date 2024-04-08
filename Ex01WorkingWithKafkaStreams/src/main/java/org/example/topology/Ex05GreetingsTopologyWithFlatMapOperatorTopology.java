package org.example.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Ex05GreetingsTopologyWithFlatMapOperatorTopology {

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

        //KStream<String, String> greetinsStream =streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));
        var greetinsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String())); //behind the scene it uses consumer api
        greetinsStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream")); //this line is just for logging/debugging purposes

        var modifiedStream = greetinsStream
//                .filterNot((key, value)-> value.length()>5)
//                .mapValues((readOnlyKey, value) -> value.toUpperCase());
//                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
        .flatMap((key,value) -> {
            var newValues = Arrays.asList(value.split(""));
            return newValues
                    .stream()
                    .map(val -> KeyValue.pair(key,val))
                    .collect(Collectors.toList());
        });
        modifiedStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream")); //this line is just for logging/debugging purposes

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String())); //behind the scene it uses the producer api

        return streamsBuilder.build();
    }
}
