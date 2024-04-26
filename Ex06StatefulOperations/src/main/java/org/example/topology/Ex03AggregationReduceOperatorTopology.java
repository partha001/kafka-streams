package org.example.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class Ex03AggregationReduceOperatorTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var inputStream = streamsBuilder
                .stream(AGGREGATE,
                        Consumed.with(Serdes.String(),Serdes.String()));

        inputStream
                .print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));

        var groupedString = inputStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        exploreReduce(groupedString);

        return streamsBuilder.build();
    }

    private static void exploreReduce(KGroupedStream<String,String> groupedStream){
        KTable<String, String> reducedStream = groupedStream.reduce((value1, value2) -> {
            log.info("value1:{} value2:{}", value1, value2);
            return value1.toUpperCase() + "-" + value2.toUpperCase();
        });

        reducedStream
                .toStream()
                .print(Printed.<String,String>toSysOut()
                        .withLabel("reduced-words"));
    }
}
