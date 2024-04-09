package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.example.topology.Ex08GreetingsTopologyWithMergeOperatorTopology;


import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.example.topology.Ex08GreetingsTopologyWithMergeOperatorTopology.*;

@Slf4j
public class Ex08GreetingsStreamAppMergeOperatorLauncher {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greeting-app" ); //this is an unique identifier for the application. can also be thought of an consumer-group name
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        //calling create topics to get the topics created
        createTopics(properties, List.of(GREETINGS_SPANISH, //passing the added spanish source here for the topic to be created
                GREETINGS,
                GREETINGS_UPPERCASE));

        // here are actaully initiating our topology
        var greetingTopology = Ex08GreetingsTopologyWithMergeOperatorTopology.buildTopoloogy();
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
