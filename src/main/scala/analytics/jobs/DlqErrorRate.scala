package analytics.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object DlqErrorRate {

  def run(implicit events: DataFrame): DataFrame = {
    val knownTypes = Set("AccountCreated", "AccountDebited", "AccountCredited")

    val totalByDay = events
      .filter(col("date").isNotNull)
      .filter(col("eventType").isin(knownTypes.toSeq: _*))
      .groupBy("date")
      .agg(count("*").alias("totalEvents"))

    val dlqByDay = events
      .filter(isMalformed(knownTypes))
      .withColumn("dlqDate", coalesce(
        to_date(col("ts")),
        to_date(to_timestamp(col("ingestedAt")))
      ))
      .groupBy(col("dlqDate").alias("date"))
      .agg(count("*").alias("dlqEvents"))

    totalByDay
      .join(dlqByDay, Seq("date"), "left")
      .withColumn("dlqEvents", coalesce(col("dlqEvents"), lit(0)))
      .withColumn("errorRatePct",
        round((col("dlqEvents") / col("totalEvents")) * 100, 2)
      )
      .select("date", "totalEvents", "dlqEvents", "errorRatePct")
      .orderBy("date")
  }

  private def isMalformed(knownTypes: Set[String]): Column = (
    !col("eventType").isin(knownTypes.toSeq: _*)
      || col("accountId").isNull
      || col("ts").isNull
    )

}
