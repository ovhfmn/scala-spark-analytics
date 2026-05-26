package analytics.jobs

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, sum, to_date, when}

/** Reconstructs the running account balance for every account over time.
 *
 * Treats AccountCreated.initialBalance as the opening delta, applies
 * AccountDebited events as negative deltas and AccountCredited events
 * as positive deltas, ordered by timestamp within each account partition.
 *
 * Key technique: cumulative sum via Spark window function partitioned
 * by accountId and ordered by ts — distributed, safe at scale.
 *
 * Input:  raw events DataFrame from [[analytics.schema.EventSchema]]
 * Output: output/balance-trends/
 */
object BalanceTrends {

  /**
   *
   * @param events raw events DataFrame produced by [[analytics.schema.EventSchema]]
   * @return DataFrame w/ columns: accountId ts date eventType delta runningBalance
   */
  def run(implicit events: DataFrame): DataFrame = {
    val created = events
      .filter(col("eventType") === "AccountCreated")
      .select(
        col("accountId"),
        col("ts"),
        col("initialBalance").alias("delta"),
        col("eventType")
      )

    val movements = events
      .filter(col("eventType").isin("AccountDebited", "AccountCredited"))
      .withColumn("delta",
        when(col("eventType") === "AccountDebited", col("amount") * -1)
          .otherwise(col("amount"))
      )
      .select(
        col("accountId"),
        col("ts"),
        col("delta"),
        col("eventType")
      )

    val allMovements = created.union(movements)

    val windowSpec = Window
      .partitionBy("accountId")
      .orderBy("ts")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    allMovements
      .withColumn("runningBalance", sum("delta").over(windowSpec))
      .withColumn("date", to_date(col("ts")))
      .select("accountId", "ts", "date", "eventType", "delta", "runningBalance")
      .orderBy("accountId", "ts")
  }
}
