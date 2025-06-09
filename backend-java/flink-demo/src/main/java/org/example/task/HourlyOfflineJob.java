package org.example.task;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.example.domain.DriverEvent;
import org.example.sink.ParquetS3Sink;
import org.example.source.SourceFactory;
import org.example.util.EventTransforms;
import org.example.util.JsonUtil;

import java.time.Duration;

public class HourlyOfflineJob {

    public static void main(String[] args) {
        System.out.println("Starting Heatmap Offline Job...");

        // Initialize the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        // Define the data source, from kafka topic "driver-logs"
        DataStreamSource<String> raw = env.fromSource(
                SourceFactory.kafka("geo-heatmap-consumer-offline-v1",
                        OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        DataStream<DriverEvent> events = JsonUtil.toDriverEvent(raw);

        // Assign 15 minutes of out-of-orderness for event time
        WatermarkStrategy<DriverEvent> wm = WatermarkStrategy
                .<DriverEvent>forBoundedOutOfOrderness(Duration.ofMinutes(15))
                .withTimestampAssigner((event, timestamp) -> event.timestamp);


        events
                .assignTimestampsAndWatermarks(wm) // <= 15 minutes of out-of-orderness
                .map(EventTransforms.toGeoHour()) // Convert to (geoHash, windowStart, count) for hourly heatmap
                .returns(EventTransforms.GEO_HOUR_TYPE)
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.hours(1).toDuration()))
                .allowedLateness(Time.minutes(10).toDuration()) // Allow late events up to 10 minutes after the window closes
                .sum(2) // Sum 3rd field (count) in the tuple
                .sinkTo((Sink<Tuple3<String, Long, Long>, ?, ?, ?>) ParquetS3Sink.build());

        try {
            env.execute("Geo Heatmap - Offline 1-Hour");
        } catch (Exception e) {
            System.err.println("Error executing Flink job: " + e.getMessage());
        }
    }
}
