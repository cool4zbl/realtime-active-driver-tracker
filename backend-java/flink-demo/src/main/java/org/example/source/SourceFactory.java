package org.example.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class SourceFactory {
    private static final String BROKERS = "localhost:9092";
    private static final String TOPIC = "driver-logs";

    public static KafkaSource<String> kafka(String group, OffsetsInitializer offsets) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(BROKERS)
                .setTopics(TOPIC)
                .setGroupId(group)
                // 1. 实时作业：latest()，部署快，不拖积压
                // 2. 离线回填作业：单独启动一个 job，earliest() → 处理完写 HDFS／S3，然后关掉。
                // 首次上线且必须补历史 → 可以先用 earliest() 全量跑一次，再把消费者 group 重置到最新 offset，再切到 latest()
                .setStartingOffsets(offsets)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    private SourceFactory() {}
}
