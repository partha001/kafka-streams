package org.example.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.domain.GreetingMessage;
import org.example.serdes.SerdesFactory;

public class Ex02CustomSerdeTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";




    public static Topology buildTopoloogy(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var mergedStream = getCustomGreetingMessageKStream(streamsBuilder);
        mergedStream.print(Printed.<String,GreetingMessage>toSysOut().withLabel("mergedStream"));

        var modifiedStream = mergedStream.mapValues(((readOnlyKey, value) ->
                 new GreetingMessage(value.getMessage().toUpperCase(), value.getTimestamp())));

        //here we are specifying the Serdes for writing to the target topic
        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.greetingMessageSerde()));
        return streamsBuilder.build();
    }



    private static KStream<String, GreetingMessage> getCustomGreetingMessageKStream(StreamsBuilder streamsBuilder){

        //note that the key we are still reading with string. its only the value that
        // we are serializing/deserializing using our custom serializer
        var greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.greetingMessageSerde()));


        var greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdesFactory.greetingMessageSerde()));

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);

        return mergedStream;
    }
}
