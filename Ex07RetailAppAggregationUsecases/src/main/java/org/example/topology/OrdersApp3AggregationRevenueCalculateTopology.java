package org.example.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.example.domain.Order;
import org.example.domain.OrderType;
import org.example.domain.Revenue;
import org.example.domain.TotalRevenue;
import org.example.serdes.SerdesFactory;


@Slf4j
public class OrdersApp3AggregationRevenueCalculateTopology {
    public static final String ORDERS = "orders";

    public static final String STORES = "stores";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";

    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";

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

                    aggregateOrderCountByLocationId(generalOrdersStream, GENERAL_ORDERS_COUNT);
                    aggregateOrderRevenueByLocationId(generalOrdersStream, GENERAL_ORDERS_REVENUE);

                }))
                .branch(restaurantPredicate, Branched.withConsumer(restaurantOrderStreams -> {
                    //any processing logic for restaurant orders can be inserted here
                    restaurantOrderStreams.print(Printed.<String, Order>toSysOut().withLabel("restaurant stream"));//printing out just for debugging purposes

                    aggregateOrderCountByLocationId(restaurantOrderStreams, RESTAURANT_ORDERS_COUNT);
                    aggregateOrderRevenueByLocationId(restaurantOrderStreams, RESTAURANT_ORDERS_REVENUE);
                }));


        return streamsBuilder.build();
    }

    private static void aggregateOrderRevenueByLocationId(KStream<String, Order> generalOrdersStream, String storeName) {
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator = (key, value, aggregate) -> aggregate.updateRunningRevenue(key, value);

        KTable<String, TotalRevenue> revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerde()));

        revenueTable.toStream()
                .print(Printed.<String, TotalRevenue>toSysOut().withLabel(storeName));
    }

    private static void aggregateOrderCountByLocationId(KStream<String, Order> generalOrdersStream, String storeName) {
        //since initally the orders that are posted by the producer have a key or order id.
        // so we are using map operator to change the key to locationId
        KTable<String, Long> ordersCountPerStore = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerde()))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore.toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(storeName));
    }





}
