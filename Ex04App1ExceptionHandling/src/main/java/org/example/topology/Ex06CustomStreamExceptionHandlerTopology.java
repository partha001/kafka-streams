package org.example.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.example.domain.GreetingMessage;
import org.example.serdes.SerdesFactory;

@Slf4j
public class Ex06CustomStreamExceptionHandlerTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_SPANISH = "greetings_spanish";

    public static String GREETINGS_UPPERCASE = "greetings_uppercase";


    public static Topology buildTopoloogy() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var mergedStream = getCustomGreetingMessageKStream(streamsBuilder);
        mergedStream.print(Printed.<String, GreetingMessage>toSysOut().withLabel("mergedStream"));

        //        var modifiedStream = mergedStream.mapValues(((readOnlyKey, value) ->
//                new GreetingMessage(value.getMessage().toUpperCase(), value.getTimestamp())));

        KStream<String, GreetingMessage> modifiedStream = exploreErrors(mergedStream);

        modifiedStream.to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), SerdesFactory.greetingSerdesusingGenerics()));
        return streamsBuilder.build();
    }


    private static KStream<String, GreetingMessage> exploreErrors(KStream<String, GreetingMessage> mergedStream) {
        return mergedStream.mapValues((readOnlyKey, value) -> {
            if (value.getMessage().equals("Transient Error")) {
                try {
                    throw new IllegalStateException(value.getMessage());
                }catch(Exception e) {
                    log.error("exception is explore errors : {} ", value.getMessage(),e);
                    return null;
                }
            }
            return new GreetingMessage(value.getMessage().toUpperCase(), value.getTimestamp());
        }).filter((key,value)-> key!=null && value!=null);
    }


    private static KStream<String, GreetingMessage> getCustomGreetingMessageKStream(StreamsBuilder streamsBuilder) {

        var greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesusingGenerics()));


        var greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), SerdesFactory.greetingSerdesusingGenerics()));

        var mergedStream = greetingsStream.merge(greetingsSpanishStream);

        return mergedStream;
    }
}
