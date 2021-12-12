package chapter09

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class ParquetFileReadWrite extends AnyFunSuite {
    test ("parquet_read") {
        val spark = CommonSpark.createLocalSparkSession("test")
        spark.read
          .format("parquet")
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet")
          .show(3)
        /*
        +-----------------+-------------------+-----+
        |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
        +-----------------+-------------------+-----+
        |    United States|            Romania|    1|
        |    United States|            Ireland|  264|
        |    United States|              India|   69|
        +-----------------+-------------------+-----+
         */
    }

    test ("parquet_write") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq(
            (1, 2, 3)
        ).toDF("a", "b", "c")

        df.write
          .format("parquet")
          .mode(SaveMode.Overwrite)
          .save("/tmp/my-parquet-file.parquet")
    }
}
