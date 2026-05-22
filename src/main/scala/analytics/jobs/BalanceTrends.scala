package analytics.jobs

import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.{col, to_date, when}

object BalanceTrends {

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
      .filter(col("eventType").isin("AccountDebited","AccountCredited"))
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
      .select("accountId","ts","date","eventType","delta","runningBalance")
      .orderBy("accountId","ts")
  }
}
