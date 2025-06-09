package org.example.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

public final class ParquetS3Sink {
    private ParquetS3Sink() {
        // Private constructor to prevent instantiation
    }

    public static Sink<GenericRecord> build() {
        Schema avroSchema = AvroSchemaConverter.convertToSchema(
                DataTypes.ROW(
                        DataTypes.FIELD("window_start", DataTypes.TIMESTAMP_LTZ()),
                        DataTypes.FIELD("geohash",       DataTypes.STRING()),
                        DataTypes.FIELD("count",         DataTypes.BIGINT())
                ).getLogicalType()
        );

        return FileSink
                .forBulkFormat(
                        new Path("s3a://heatmap-offline/"),
                        ParquetAvroWriters.forGenericRecord(avroSchema)
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy/MM/dd/HH"))
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("part")
                                .withPartSuffix(".snappy.parquet")
                                .build()
                )
                .build();
    }
}
