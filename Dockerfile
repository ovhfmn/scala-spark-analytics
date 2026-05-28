FROM eclipse-temurin:17-jre-jammy

WORKDIR /app

COPY target/scala-2.13/scala-spark-analytics-assembly-0.1.0-SNAPSHOT.jar scala-spark-analytics.jar

ENV INPUT_PATH=data/events-*.jsonl

ENV OUTPUT_PATH=output

ENTRYPOINT ["java", \
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
  "--add-opens=java.base/java.nio=ALL-UNNAMED", \
  "--add-opens=java.base/java.util=ALL-UNNAMED", \
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED", \
  "-Dspark.driver.bindAddress=127.0.0.1", \
  "-Dspark.driver.port=7077", \
  "-jar", "scala-spark-analytics.jar"]