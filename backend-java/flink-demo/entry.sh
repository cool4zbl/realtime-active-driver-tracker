export FLINK_VERSION=1.20.1
export MAIN_CLASS=org.example.WindowWordCount     # 你的入口类
export JAR_PATH=./target/flink-demo-1.0-SNAPSHOT.jar  # JAR包路径

docker run --rm \
  -p 8081:8081 \
  -v ${JAR_PATH}:/opt/flink/usrlib/flink-demo-1.0-SNAPSHOT.jar \
  flink:${FLINK_VERSION} \
  standalone-job
