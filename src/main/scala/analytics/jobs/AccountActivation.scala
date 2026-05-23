package analytics.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, min, percentile, unix_timestamp}

object AccountActivation {

  def run(implicit events: DataFrame): DataFrame = {
    val createdAt = events
      .filter(col("eventType") === "AccountCreated")
      .select(col("accountId"), col("ts").alias("createdAt"))

    val firstDebit = events
      .filter(col("eventType") === "AccountDebited")
      .groupBy("accountId")
      .agg(min("ts").alias("firstDebitAt"))

    val activation = createdAt.join(firstDebit, Seq("accountId"), "inner")
      .withColumn("hoursToFirstDebit", (unix_timestamp(col("firstDebitAt")) - unix_timestamp(col("createdAt"))) / 3600)

    activation.agg(
      percentile(col("hoursToFirstDebit"), lit(0.5)).alias("median_hours"),
      percentile(col("hoursToFirstDebit"), lit(0.9)).alias("p90_hours"),
      percentile(col("hoursToFirstDebit"), lit(0.99)).alias("p99_hours")
    )
  }
}
