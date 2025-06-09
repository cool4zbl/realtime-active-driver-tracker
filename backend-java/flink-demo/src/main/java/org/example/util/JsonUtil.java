package org.example.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.example.domain.DriverEvent;

public class JsonUtil {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static DataStream<DriverEvent> toDriverEvent(DataStream<String> raw) {
        return raw.map(json -> {
            try {
                return objectMapper.readValue(json, DriverEvent.class);
            } catch (Exception e) {
                System.err.println("Error deserializing JSON to DriverEvent: " + e.getMessage());
                return null; // Handle error appropriately in production code
            }
        }).filter(event -> event != null); // Filter out any null events due to deserialization errors
    }

}
