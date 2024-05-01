package org.example.serdes;


import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.example.domain.Alphabet;
import org.example.domain.AlphabetWordAggregate;

public class SerdesFactory {



    public static Serde<AlphabetWordAggregate> alphabetWordAggregate() {

        JsonSerializer<AlphabetWordAggregate> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<AlphabetWordAggregate> jsonDeSerializer
                = new JsonDeserializer<>(AlphabetWordAggregate.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }


    public static Serde<Alphabet> alphabet() {

        JsonSerializer<Alphabet> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<Alphabet> jsonDeSerializer = new JsonDeserializer<>(Alphabet.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }
}
