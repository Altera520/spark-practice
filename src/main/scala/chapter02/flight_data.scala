package chapter02

import org.apache.commons.lang3.StringUtils
import common.CommonSpark.{createLocalSparkSession, createYarnSparkSession}
import org.apache.spark.sql.{DataFrame, SparkSession}
import common.CommonUtil.{P, watchTime}

object flight_data {
    def appName = this.getClass.getName.replace("$", StringUtils.EMPTY)

    def read(spark: SparkSession) = {
        val df = spark
          .read
          .option("inferSchema", "true")  // 스키마 추론
          .option("header", "true")
          .csv(s"${P.rawDataPath}/data/flight-data/csv/2015-summary.csv")
        df
    }

    def processing(df: DataFrame) = {
        df.sort("count")
          .take(3)
    }

    def main(args: Array[String]): Unit = {
        watchTime(appName) {
            val spark = createYarnSparkSession(appName)
            val pipeline = read _ andThen processing
            pipeline(spark)
        }
    }
}
