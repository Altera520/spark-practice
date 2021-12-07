package chapter06

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.scalatest.funsuite.AnyFunSuite

class NullMul extends AnyFunSuite{
    test("null") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        // 지정한 모든 컬럼들이 null 값이 아닌 값들을 가지면 첫번째 컬럼을 반환, 여기서는 Description을 반환
        df.select(coalesce(col("Description"), col("CustomerId")))
          .show(5, false)
        /*
        +---------------------------------+
        |coalesce(Description, CustomerId)|
        +---------------------------------+
        |GROOVY CACTUS INFLATABLE         |
        |RED FLOCK LOVE HEART PHOTO FRAME |
        |LOVE HEART TRINKET POT           |
        |SAVOY ART DECO CLOCK             |
        |SET OF 3 BIRD LIGHT PINK FEATHER |
        +---------------------------------+
         */
    }

    test("drop") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        println(df.na.drop().count())                                                     // 1373, 디폴트는 any
        println(df.na.drop("any").count())                                          // 1373
        println(df.na.drop("all").count())                                          // 1560
        println(df.na.drop("all", Seq("StockCode", "InvoiceNo")).count())           // 1560
    }

    test("fill") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark.range(2)
          .select(lit(null).cast(StringType) as "val")          // StringType의 null 값으로 만든다.
          //.select(lit(null).cast("string") as "val")
          .withColumn("val_fill", col("val"))
        df.printSchema()

        df.na.fill("NaN", Seq("val_fill"))      // value값이랑 col의 타입이 불일치하면 null 값이 fill된다.(주의)
          .show()
        /*
        +----+--------+
        | val|val_fill|
        +----+--------+
        |null|     NaN|
        |null|     NaN|
        +----+--------+
         */
    }

    test("replace") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.range(10)
          .withColumn("st", lit("test"))
          .na.replace("st", Map("test" -> "empty"))

        df.show(false)
        /*
        +---+-----+
        |id |st   |
        +---+-----+
        |0  |empty|
        |1  |empty|
        +---+-----+
         */
    }
}
