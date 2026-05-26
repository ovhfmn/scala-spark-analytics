package analytics.jobs

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

private[this] case class OverdraftRow(
                                       accountId: String,
                                       runningBalance: java.math.BigDecimal,
                                       eventType: String,
                                       ts: java.sql.Timestamp,
                                       date: String,
                                       delta: java.math.BigDecimal
                                     )

/** Identifies accounts that crossed the zero balance threshold.
 *
 * Uses a typed Dataset[OverdraftRow] layer for initial filtering and
 * typed reduceGroups for per-account aggregation — demonstrating Spark's
 * Dataset API alongside the DataFrame API used in other jobs.
 *
 * Input:  BalanceTrends Delta output → output/balance-trends/
 * Output: output/overdraft-analysis/
 */
object OverdraftAnalysis {

  /**
   * Computes per account:
   *   - overdraftStartedAt: first moment balance went negative
   *   - lastPositiveAt:     last positive balance before overdraft
   *   - maxOverdraftDepth:  deepest negative balance reached
   *   - hoursInOverdraft:   duration from last positive to first negative
   *   - recovered:          whether the account returned to positive balance
   *
   * @param events Delta output produced by [[analytics.jobs.BalanceTrends]]
   * @param spark
   * @return accountId lastPositiveAt overdraftStartedAt maxOverdraftDepth hoursInOverdraft latestBalance recovered
   */
  def run(events: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val overdraftEntries: Dataset[OverdraftRow] = events
      .filter(col("runningBalance") < 0)
      .as[OverdraftRow]

    val firstOverdraft = overdraftEntries
      .filter(col("runningBalance") < 0)
      .groupByKey(_.accountId)
      .reduceGroups { (a, b) =>
        if (a.runningBalance.compareTo(b.runningBalance) < 0) a else b
      }
      .map {
        case (accountId, row) => (accountId, row.runningBalance, row.ts)
      }
      .toDF("accountId", "maxOverdraftDepth", "overdraftStartedAt")

    val lastPositive = events
      .filter(col("runningBalance") >= 0)
      .join(
        firstOverdraft.select("accountId", "overdraftStartedAt"),
        Seq("accountId"), "inner"
      )
      .filter(col("ts") < col("overdraftStartedAt"))
      .groupBy("accountId")
      .agg(max("ts").alias("lastPositiveAt"))

    val crossed = firstOverdraft
      .join(lastPositive, Seq("accountId"), "inner")
      .withColumn(
        "hoursInOverdraft",
        round(
          (unix_timestamp(col("overdraftStartedAt")) -
            unix_timestamp(col("lastPositiveAt"))) / 3600,
          2
        )
      )

    val currentBalance = events
      .groupBy("accountId")
      .agg(
        max_by(
          col("runningBalance"),
          col("ts")).alias("latestBalance")
      )

    crossed
      .join(currentBalance, Seq("accountId"), "left")
      .withColumn("recovered", col("latestBalance") >= 0)
      .withColumn("maxOverdraftDepth", round(col("maxOverdraftDepth"), 2))
      .select(
        col("accountId"),
        col("lastPositiveAt"),
        col("overdraftStartedAt"),
        col("maxOverdraftDepth"),
        col("hoursInOverdraft"),
        col("latestBalance"),
        col("recovered")
      )
      .orderBy("overdraftStartedAt")
  }
}