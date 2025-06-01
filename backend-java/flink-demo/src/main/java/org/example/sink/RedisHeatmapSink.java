package org.example.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisHeatmapSink extends RichSinkFunction<Tuple3<String, Long, Long>> {

    private transient JedisPool pool;

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig config = new JedisPoolConfig();
        pool = new JedisPool(config, "localhost", 6379);

        System.out.println("RedisHeatmapSink opened, JedisPool initialized.");
    }

    @Override
    public void close() {
        if (pool != null) {
            pool.close();
            System.out.println("RedisHeatmapSink closed, JedisPool released.");
        }
    }

    @Override
    public void invoke(Tuple3<String, Long, Long> value, Context context) {
        String geoHash = value.f0; // GeoHash or identifier for the heatmap
        long minuteTs = value.f1; // Timestamp for the heatmap data, windowStart
        long cnt = value.f2; // Count of events in this heatmap cell

        String key = "heatmap:" + minuteTs + ":" + geoHash;
        try (Jedis jedis = pool.getResource()) {
            jedis.setex(key, 1200, geoHash); // 20 minutes TTL
            System.out.println("Stored heatmap data for key: " + key + " with count: " + cnt);

        } catch (Exception e) {
            System.err.println("Error storing heatmap data in Redis: " + e.getMessage());
        }
    }

}
