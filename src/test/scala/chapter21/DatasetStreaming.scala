package chapter21

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

class DatasetStreaming extends AnyFunSuite{
    test("dataset") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        import spark.implicits._

        val dataPath = s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/"

        val dataSchema = spark.read
          .parquet(dataPath)
          .schema

        val ds = spark.readStream
          .schema(dataSchema)
          .parquet(dataPath)
          .as[Flight]

        val f = (row: Flight) => {
            row.ORIGIN_COUNTRY_NAME == row.DEST_COUNTRY_NAME
        }

        ds.filter(f)
          .groupByKey(_.DEST_COUNTRY_NAME)
          .count()
          .writeStream
          .format("memory")
          .outputMode("complete")
          .start()
          .awaitTermination()
    }
}
