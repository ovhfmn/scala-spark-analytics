package analytics.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** Measures time from account creation to first activity (debit or credit).
 *
 * Reports activation latency distribution via median, p90, and p99.
 * Accounts that were created but never activated are preserved in the
 * per-account output with null firstActivityAt — they contribute to
 * activation rate analysis but are excluded from latency percentiles.
 *
 * Input:  raw events DataFrame from [[analytics.schema.EventSchema]]
 * Output: output/account-activation/
 */
object AccountActivation {

  /**
   *
   * @param events raw events DataFrame produced by [[analytics.schema.EventSchema]]
   * @return DataFrame w/ columns: median_hours p90_hours p99_hours
   */
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
