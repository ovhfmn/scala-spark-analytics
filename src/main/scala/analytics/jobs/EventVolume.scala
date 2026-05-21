package analytics.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, hour}

object EventVolume {

  def run(implicit events: DataFrame): DataFrame = {
    val eventCount = count("*").alias("eventCount")

    events
      .withColumn("hour", hour(col("ts")))
      .groupBy("date", "hour","eventType")
      .agg(eventCount)
      .orderBy("date","hour","eventCount")
  }

}
