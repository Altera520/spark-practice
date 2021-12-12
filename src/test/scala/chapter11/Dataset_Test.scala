package chapter11

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class Dataset_Test extends AnyFunSuite {
    test("read_dataset") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val flightDF = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightDF.as[Flight]
        flights.show(2)
        /*
        +-----------------+-------------------+-----+
        |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
        +-----------------+-------------------+-----+
        |    United States|            Romania|    1|
        |    United States|            Ireland|  264|
        +-----------------+-------------------+-----+
         */
    }

    test("filtering") {
        def originDestination(flight_row: Flight): Boolean = {
            flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
        }
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val flightDF = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightDF.as[Flight]

        println(flights.filter(originDestination _).first().toString)
        // Flight(United States,United States,348113)
    }

    test("mapping") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val flightDF = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightDF.as[Flight]
        val destinations = flights.map(f => f.DEST_COUNTRY_NAME) // Dataset[String]
    }

    test("joinwith") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val flightDF = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightDF.as[Flight]
        flights.show(2, false)
        /*
        +-----------------+-------------------+-----+
        |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
        +-----------------+-------------------+-----+
        |United States    |Romania            |1    |
        |United States    |Ireland            |264  |
        +-----------------+-------------------+-----+
         */

        val flightsMeta = spark.range(500)
          .map((_, Random.nextLong))
          .withColumnRenamed("_1", "count")
          .withColumnRenamed("_2", "randomData")
          .as[FlightMetadata]
        flightsMeta.show(2, false)
        /*
        +-----+--------------------+
        |count|randomData          |
        +-----+--------------------+
        |0    |3002462711782555069 |
        |1    |-5599216110383927151|
        +-----+--------------------+
         */

        val flights2 = flights.joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))
        flights2.show(2, false)
        /*
        +---------------------------------+------------------------+
        |_1                               |_2                      |
        +---------------------------------+------------------------+
        |{United States, Uganda, 1}       |{1, 2458839915498315956}|     _1은 Flight, _2는 FlightMetadata
        |{United States, French Guiana, 1}|{1, 2458839915498315956}|
        +---------------------------------+------------------------+
         */

        flights2.selectExpr("_1.DEST_COUNTRY_NAME").show()
        /*
        +-----------------+
        |DEST_COUNTRY_NAME|
        +-----------------+
        |    United States|
        |    United States|
        |         Bulgaria|
        +-----------------+
         */
    }

    test("join") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val flightDF = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightDF.as[Flight]

        val flightsMeta = spark.range(500)
          .map((_, Random.nextLong))
          .withColumnRenamed("_1", "count")
          .withColumnRenamed("_2", "randomData")
          .as[FlightMetadata]

        val flights2 = flights.join(flightsMeta, Seq("count"))
        flights2.show(2, false)     // DF 반환, JVM 타입 정보를 모두 일어버리게 된다
        /*
        +-----+-----------------+-------------------+------------------+
        |count|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|randomData        |
        +-----+-----------------+-------------------+------------------+
        |1    |United States    |Uganda             |944118203784693960|
        |1    |United States    |French Guiana      |944118203784693960|
        +-----+-----------------+-------------------+------------------+
         */
    }

    test("aggregation") {
        def sum2(left: Flight, right: Flight) = {
            Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
        }
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val flightDF = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightDF.as[Flight]

        flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups(sum2 _).take(2)
    }
}
