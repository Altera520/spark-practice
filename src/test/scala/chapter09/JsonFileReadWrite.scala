package chapter09

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class JsonFileReadWrite extends AnyFunSuite {
    test ("json_read") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val manualSchema = StructType(
            Seq(
                // 소문자 origin_country_name을 지정하면 로우 값이 null -> 대소문자를 구분한다.
                StructField("ORIGIN_COUNTRY_NAME", StringType, true),
                StructField("DEST_COUNTRY_NAME", StringType, true),
                StructField("count", LongType, false),
            )
        )

        // json file read
        spark.read
          .format("json")
          .option("mode", "FAILFAST")
          .schema(manualSchema)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2010-summary.json")
          .show(3)
        /*
        +-------------------+-----------------+-----+
        |ORIGIN_COUNTRY_NAME|DEST_COUNTRY_NAME|count|
        +-------------------+-----------------+-----+
        |            Romania|    United States|    1|
        |            Ireland|    United States|  264|
        |              India|    United States|   69|
        +-------------------+-----------------+-----+
         */
    }

    test ("json_write") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq((1, 2, 3), (1, 4, 3))
          .toDF("a", "b", "c")
        df.show()

        // json file read
        df.write
          .format("json")
          .mode(SaveMode.Overwrite)
          .save("/tmp/my-json-file.json")
    }
}
