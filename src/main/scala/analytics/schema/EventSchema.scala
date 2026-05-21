package analytics.schema

import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}

object EventSchema {

   val accountEvent: StructType = StructType(Seq(
    StructField("eventType", StringType, nullable = false),
    StructField("eventId", StringType, nullable = false),
    StructField("occurredAt", StringType, nullable = false),
    StructField("accountId", StringType, nullable = false),
    StructField("initialBalance", DecimalType(18, 2), nullable = true),
    StructField("amount", DecimalType(18, 2), nullable = true)
  ))

  def read(path: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .schema(accountEvent)
      .json(path)
      .withColumn("ts", to_timestamp(col("occurredAt")))
      .withColumn("date", to_date(col("ts")))

  }

}
