package chapter03

import common.{CommonSpark, CommonUtil}
import common.CommonUtil.{P, watchTime}
import org.apache.spark.sql.functions.{col, window}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object retail_data {

    val appName = CommonUtil.getAppName(this)

    def read(spark: SparkSession) = {
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")
        df
    }

    def processing(df: DataFrame) = {
        df.createOrReplaceTempView("retail_data")
        val staticSchema = df.schema

        df.selectExpr(
            "CustomerId",
            "(UnitPrice * Quantity) as total_cost",
            "InvoiceDate")
          .groupBy(
              col("CustomerId"),
              window(col("InvoiceDate"), "1 day")
          )
          .sum("total_cost")
    }

    def write(df: DataFrame) = {
        df
    }

    def main(args: Array[String]): Unit = {
        watchTime(appName) {
            val spark = CommonSpark.createYarnSparkSession(appName)
            val pipe = read _ andThen processing andThen write
            pipe(spark)
        }
    }
}
