package chapter09

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class AdvancedIO extends AnyFunSuite {
    test("parallel_write") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq(
            (1, 2, 3),
            (1, 2, 3),
            (1, 2, 3),
            (1, 2, 3),
            (1, 2, 3)
        ).toDF("a", "b", "c")

        df.repartition(5)       // write하기 전에 파티션 수를 조절, 파티션 1개당 1개의 파일
          .write
          .format("csv")
          .save("/tmp/multiple.csv")
    }

    test("partitioning") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/csv/2010-summary.csv")

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .partitionBy("DEST_COUNTRY_NAME")     // 파티션 지정
          .save("/tmp/partitioned-files.parquet")
    }

    test("bucketing") {
        val numberBuckets = 10
        val columnToBucketBy = "count"
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/csv/2010-summary.csv")

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .bucketBy(numberBuckets, columnToBucketBy)
    }
}
