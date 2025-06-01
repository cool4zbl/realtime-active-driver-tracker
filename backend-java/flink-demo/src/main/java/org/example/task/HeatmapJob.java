package org.example.task;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class HeatmapJob {
    public static void main(String[] args) {
        System.out.print("Starting Geo Heatmap Streaming Job...");
        // Initialize the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the data source, from kafka topic "driver-logs"
        FlinkKafkaConsumer<String> consumer = getStringFlinkKafkaConsumer();

        env.addSource(consumer)
                .map(value -> "Received: " + value)
                .returns(String.class)
                .print();

        // Execute the Flink job
        try {
            env.execute("Geo Heatmap Streaming Job");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static FlinkKafkaConsumer<String> getStringFlinkKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "geo-heatmap-consumer");

        // Create a FlinkKafkaConsumer to read from the Kafka topic
//        {
//                "driver_id": "driver-654",
//                "event_type": "location_update",
//                "lat": 51.571783,
//                "lon": 4.785065,
//                "timestamp": 1748114647
//        }
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "driver-logs",
                new SimpleStringSchema(),
                props
        );

        consumer.setStartFromEarliest();

        // Process the input data to create a heatmap
//        input
//            .map(line -> {
//                String[] parts = line.split(",");
//                return new HeatmapData(parts[0], Double.parseDouble(parts[1]), Double.parseDouble(parts[2]));
//            })
//            .keyBy(HeatmapData::getLocation)
//            .sum("intensity")
//            .print();

        return consumer;
    }
}
