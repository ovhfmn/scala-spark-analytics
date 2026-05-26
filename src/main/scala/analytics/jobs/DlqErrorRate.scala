package analytics.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/** Computes daily malformed event rate against total valid event volume.
 *
 * Identifies malformed events by unknown eventType, null accountId,
 * or null ts. Malformed events are dated by ingestedAt rather than
 * occurredAt — the event's own timestamp cannot be trusted when the
 * payload is broken.
 *
 * Input:  raw events DataFrame from [[analytics.schema.EventSchema]]
 * Output: output/dlq-error-rate/
 */
object DlqErrorRate {

  /**
   *
   * @param events raw events DataFrame produced by [[analytics.schema.EventSchema]]
   * @return DataFrame w/ columns: date totalEvents dlqEvents errorRatePct
   */
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
