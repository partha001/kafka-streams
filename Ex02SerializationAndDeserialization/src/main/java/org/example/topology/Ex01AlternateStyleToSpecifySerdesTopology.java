package org.example.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class Ex01AlternateStyleToSpecifySerdesTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";


    public static Topology buildTopoloogy(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        /** earlier we took this approach to mention the serializer and deserializer**/
        //var greetinsStream = streamsBuilder.stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> greetinsStream = streamsBuilder.stream(GREETINGS); //passing serdes rather passing default serializer/deserializer from the launcher


        greetinsStream.print(Printed.<String,String>toSysOut().withLabel("greetingsStream")); //this line is just for logging/debugging purposes

        var modifiedStream = greetinsStream.mapValues((readOnlyKey, value) -> value.toUpperCase());
        modifiedStream.print(Printed.<String,String>toSysOut().withLabel("greetingsStream")); //this line is just for logging/debugging purposes

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String())); //behind the scene it uses the producer api

        return streamsBuilder.build();
    }
}
