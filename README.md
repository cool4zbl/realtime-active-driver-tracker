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