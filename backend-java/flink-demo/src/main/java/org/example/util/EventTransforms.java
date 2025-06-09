package org.example.util;

import ch.hsr.geohash.GeoHash;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.example.domain.DriverEvent;

public class EventTransforms {
    public static final TypeInformation<Tuple2<String, Long>> GEO_MINUTE_TYPE =
            Types.TUPLE(
                    Types.STRING, // GeoHash
                    Types.LONG
            );
    public static final TypeInformation<Tuple3<String, Long, Long>> GEO_HOUR_TYPE =
            Types.TUPLE(Types.STRING, Types.LONG, Types.LONG);

    public static MapFunction<DriverEvent, Tuple2<String, Long>> toGeoMinute() {
        return e -> Tuple2.of(
                GeoHash.geoHashStringWithCharacterPrecision(e.lat, e.lon, 6),
                // 对齐到整分钟的 epochMillis（形如 1717523640000）, 同时保留 时间含义，写入 Redis / 日志更直观
                (e.timestamp / 60000) * 60000 // Convert timestamp to minute granularity
        );
    }
    public static MapFunction<DriverEvent, Tuple3<String, Long, Long>> toGeoHour() {
        return e -> Tuple3.of(
                GeoHash.geoHashStringWithCharacterPrecision(e.lat, e.lon, 6),
                (e.timestamp / 3600000) * 3600000,
                1L // Count of events in this heatmap cell, can be 1 for each event
        );
    }
}
