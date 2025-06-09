package org.example.task;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.domain.DriverEvent;
import org.example.sink.RedisHeatmapSink;
import org.example.source.SourceFactory;
import org.example.util.EventTransforms;
import org.example.util.JsonUtil;

import java.time.Duration;

public class RealtimeJob {
    public static void main(String[] args) {
        System.out.println("Starting Heatmap Realtime Job...");

        // Initialize the Flink execution environment
         StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
         env.enableCheckpointing(10_000);
         env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        // Define the data source, from kafka topic "driver-logs"
        DataStreamSource<String> raw = env.fromSource(
                SourceFactory.kafka("geo-heatmap-consumer-realtime-v1",
                        OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
        );

        DataStream<DriverEvent> events = JsonUtil.toDriverEvent(raw);

        // Assign 10 seconds of out-of-orderness for event time
        WatermarkStrategy<DriverEvent> wm = WatermarkStrategy
                .<DriverEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.timestamp);

        events
                .assignTimestampsAndWatermarks(wm)
                .map(EventTransforms.toGeoMinute())
                .returns(EventTransforms.GEO_MINUTE_TYPE)
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1).toDuration()))
                .aggregate(new CountAgg(), new WindowToTuple())
                .addSink(new RedisHeatmapSink());

        try {
            env.execute("Geo Heatmap - Realtime");
        } catch (Exception e) {
            System.err.println("Error executing Flink job: " + e.getMessage());
        }
    }

    // AggregateFunction to count occurrences of each geoHash in the window
    public static class CountAgg implements AggregateFunction<
            Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator + 1; // Count occurrences
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b; // Merge counts
        }
    }

    // ProcessWindowFunction to convert the aggregated count into a Tuple3, to be used in the sink
    public static class WindowToTuple extends ProcessWindowFunction<Long, Tuple3<String, Long, Long>, String, TimeWindow> {
        @Override
        public void process(String geo,
                            Context ctx,
                            Iterable<Long> elements,
                            Collector<Tuple3<String, Long, Long>> out) {
            long cnt = elements.iterator().next(); // Get the count from the accumulator
            long minuteTs = ctx.window().getStart();
            // Emit the result as a Tuple3: (geoHash, minute timestamp, count)
            out.collect(Tuple3.of(geo, minuteTs, cnt));
        }
    }

}
