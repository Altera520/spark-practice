package chapter09

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class CsvFileWrite extends AnyFunSuite {
    test("csv_file_write") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val manualSchema = StructType(
            Seq(
                StructField("dest_country_name", StringType, true),
                StructField("origin_country_name", StringType, true),
                StructField("count", LongType, false),
            )
        )

        // csv file read
        val df = spark.read
          .format("csv")
          .option("header", true)
          .option("mode", "FAILFAST")
          .schema(manualSchema)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/csv/2010-summary.csv")

        // csv to tsv file write
        df.write
          .format("csv")
          .mode("overwrite")
          .option("sep", "\t")
          .save("/tmp/my-tsv-file.tsv")
    }
}
