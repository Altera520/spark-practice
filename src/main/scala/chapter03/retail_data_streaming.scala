package chapter03

import common.{CommonSpark, CommonUtil}
import common.CommonUtil.{P, watchTime}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, window}

object retail_data_streaming {
    val appName = CommonUtil.getAppName(this)

    def read(spark: SparkSession) = {
        val staticDF = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        val df = spark
          .readStream
          .schema(staticDF.schema)
          .format("csv")
          .option("header", "true")
          .option("maxFilesPerTrigger", 1)  // 한 번에 읽을 파일 수 지정, 운영환경에 적용하는 것은 비추천
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        println("isStreaming: " + df.isStreaming)
        df
    }

    def processing(df: DataFrame) = {
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
          .writeStream
          .format("memory")   // InMemory 테이블에 저장
          .queryName("customer_purchases")  // 인메모리에 저장될 테이블명
          .outputMode("complete")          // complete, 모든 카운트 수행 결과를 테이블에 저장
          .start()
    }

    def main(args: Array[String]): Unit = {
        watchTime(appName) {
            val spark = CommonSpark.createYarnSparkSession(appName)
            val pipe = read _ andThen processing andThen write
            pipe(spark)
        }
    }
}
