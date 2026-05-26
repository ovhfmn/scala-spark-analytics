package analytics.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, hour}

/** Counts domain events grouped by date, hour, and event type.
 *
 * Provides a time-series view of platform activity, useful for
 * capacity planning and detecting unusual traffic patterns.
 * Malformed and unknown event types are excluded before aggregation.
 *
 * Input:  raw events DataFrame from [[analytics.schema.EventSchema]]
 * Output: output/event-volume/
 */
object EventVolume {

  /**
   * @param events raw events DataFrame produced by [[analytics.schema.EventSchema]]
   * @return DataFrame w/ columns: date hour eventType eventCount
   */
  def run(implicit events: DataFrame): DataFrame = {
    val eventCount = count("*").alias("eventCount")

    events
      .filter(col("eventType").isin("AccountCreated", "AccountDebited", "AccountCredited"))
      .withColumn("hour", hour(col("ts")))
      .groupBy("date", "hour", "eventType")
      .agg(eventCount)
      .orderBy("date", "hour", "eventCount")
  }

}
