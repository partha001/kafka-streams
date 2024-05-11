package org.example.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.example.domain.Order;
import org.example.domain.OrderType;
import org.example.domain.Revenue;
import org.example.serdes.SerdesFactory;

@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";

    public static final String STORES = "stores";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";

    public static final String GENERAL_ORDERS = "general_orders";

    public static Topology buildTopology() {

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);

        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);

        ValueMapper<Order, Revenue> revenueMapper = order -> new Revenue(order.locationId(), order.finalAmount());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        var ordersStream = streamsBuilder.stream(ORDERS,
                Consumed.with(Serdes.String(), SerdesFactory.orderSerde())
        );

        ordersStream.print(Printed.<String, Order>toSysOut().withLabel("orders"));

        //thus here we are splitting one single stream based upon some predicate logic into multiple streams i.e. generalOrdersStram and restaurantOrderStream
        ordersStream.split(Named.as("General-restaurant-stream")) //passing this parameter is optional
                .branch(generalPredicate, Branched.withConsumer(generalOrdersStream -> {
                    //any processing logic for general orders can be inserted here
                    generalOrdersStream
                            .print(Printed.<String, Order>toSysOut().withLabel("general stream")); //printing out just for debugging purposes

                    generalOrdersStream
                            .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStreams -> {
                    //any processing logic for restaurant orders can be inserted here
                    restaurantOrderStreams.print(Printed.<String, Order>toSysOut().withLabel("restaurant stream"));//printing out just for debugging purposes

                    restaurantOrderStreams
                            //.to(RESTAURANT_ORDERS,Produced.with(Serdes.String(), SerdesFactory.orderSerde()));
                            .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdesFactory.revenueSerde()));

                }));


        return streamsBuilder.build();
    }
}
