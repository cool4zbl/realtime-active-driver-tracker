package org.example.task;

import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.domain.DriverEvent;
import org.example.sink.RedisHeatmapSink;

import java.time.Duration;

public class HeatmapJob {
    public static void main(String[] args) {

        System.out.print("Starting Geo Heatmap Streaming Job...");

        // Initialize the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10_000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);

        // Define the data source, from kafka topic "driver-logs"
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("driver-logs")
                .setGroupId("geo-heatmap-consumer-v1")
                // 1. 实时作业：latest()，部署快，不拖积压
                // 2. 离线回填作业：单独启动一个 job，earliest() → 处理完写 HDFS／S3，然后关掉。
                // 首次上线且必须补历史 → 可以先用 earliest() 全量跑一次，再把消费者 group 重置到最新 offset，再切到 latest()
                .setStartingOffsets(OffsetsInitializer.latest()) // Use latest for real-time processing, not to process old data
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> raw = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");
//        raw.map(x -> {
//                    System.out.println("DEBUG received from kafka source=" + x); // For debugging, remove in production
//                    return x;
//                })
//                .sinkTo(new DiscardingSink<>());


        // Process the input data to get DriverEvent objects & timestamp handling
        ObjectMapper objectMapper = new ObjectMapper();

        DataStream<DriverEvent> events = raw
                .process(new ProcessFunction<String, DriverEvent>() {
                    @Override
                    public void processElement(String line, Context ctx, Collector<DriverEvent> out) {
                        try {
                            DriverEvent e = objectMapper.readValue(line, DriverEvent.class);
//                            ctx.output(null, e);
                            out.collect(e);
                        } catch (Exception e) {
                            System.err.println("Failed to parse event: " + line + ", error: " + e.getMessage());
                        }
                    }
                })
                // Assign 10 seconds of out-of-orderness for event time
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<DriverEvent>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10))
                                .withTimestampAssigner((e, ts) -> e.timestamp)
                );

        events.map(e -> {
                    String geo = GeoHash.geoHashStringWithCharacterPrecision(e.lat, e.lon, 6);
                    // 对齐到整分钟的 epochMillis（形如 1717523640000）, 同时保留 时间含义，写入 Redis / 日志更直观
                    long minuteTs = (e.timestamp / 60000) * 60000; // Convert timestamp to minute granularity
                    return Tuple2.of(geo, minuteTs);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .aggregate(new CountAgg(), new WindowToTuple())
                .addSink(new RedisHeatmapSink());

        // Execute the Flink job
        try {
            env.execute("Geo Heatmap Job");
        } catch (Exception e) {
            e.printStackTrace();
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
