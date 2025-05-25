FROM flink:1.17-scala_2.12

COPY target/geo-heatmap-flink-1.0-SNAPSHOT.jar /opt/flink/usrlib/

CMD ["flink", "run", "-d", "-c", "org.example.HeatmapJob", "/opt/flink/usrlib/geo-heatmap-flink-1.0-SNAPSHOT.jar"]
