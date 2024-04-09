package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.example.topology.Ex02CustomSerdeTopology;


import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.example.topology.Ex02CustomSerdeTopology.*;

@Slf4j
public class Ex02UsingCostomSerdes {


    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greeting-app" );
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        //adding default serde here
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);


        //calling create topics to get the topics created
        createTopics(properties, List.of( GREETINGS, GREETINGS_SPANISH, GREETINGS_UPPERCASE));

        // here are actaully initiating our topology
        var greetingTopology = Ex02CustomSerdeTopology.buildTopoloogy();
        var kafkaStreams = new KafkaStreams(greetingTopology, properties);



        //here we are registering a graceful shutdown hook. this will also take care of clearing out the resources created by the application
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        try{
            kafkaStreams.start();
        }catch(Exception e) {
            log.error("exception in starting the stream : {}", e.getMessage(), e);
        }
    }


    /**
     * here in the below method we are basically creating kafka source and destination topics programatically
     * @param config
     * @param greetings
     */
    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }
}
