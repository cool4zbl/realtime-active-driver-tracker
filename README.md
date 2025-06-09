# Realtime Driver Tracker

### Dataflow
```
[Driver Device / Simulator]
        |
  (1) Produce location events
        |
        v
 +-----------------+         +----------+
 |   Kafka Topic   |  --->   |  Flink   |
 |   "driver-logs" |         | Aggreg.  |
 +-----------------+         +----+-----+
                              |   |
  (2) Real-time aggregates    |   | (3) Hourly aggregates
                              v   v
                        +-----------+
                        |  Redis    | (Real-time store)
                        +-----------+

       +-----------------------------------+
       |  (4) Optionally store hour data   |
       |    to S3 / SQL DB / CSV           |
       +-----------------------------------+

(5) Real-time Dashboard  <---->  Redis
(6) Batch queries (24h+) <---->  S3 / DB
```

### Components
- **Driver Device / Simulator**: Produces location events. located in `backend-python/driver-simulator/`.
- **Kafka Topic**: `driver-logs` topic where location events are published.
- ***Flink Aggregator**: Consumes events from Kafka, aggregates them in real-time, and stores results in Redis. Located in `backend-java/flink-aggregator/`.
  1. Produces real-time aggregates (e.g., current location, etc.) every 1 minute.
  2. Produces hourly aggregates (e.g., current location) every hour.
