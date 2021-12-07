package chapter06

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import scala.language.postfixOps

class StringTypeMul extends AnyFunSuite {

    test("inticap") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        // 공백으로 나뉘는 단어의 첫 글자를 대문자로
        df.select(initcap(col("Description"))).show(2, false)
        /*
        +--------------------------------+
        |initcap(Description)            |
        +--------------------------------+
        |Groovy Cactus Inflatable        |
        |Red Flock Love Heart Photo Frame|
        +--------------------------------+
         */
    }

    test("regexp_replace") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val colors = Seq("black", "white", "red", "green", "blue")
        val regexString = colors.map(_.toUpperCase()).mkString("|")

        df.select(
            regexp_replace(col("Description"), regexString, "COLOR").alias("color"),
            col("Description"))
          .show(2)
    }

    test("translate") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.select(
            translate(col("Description"), "LEET", "1337"),
            col("Description")
        ).show(2)
    }

    test("contains") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val containsBlack = col("Description").contains("BLACK")
        val containsWhite = col("Description").contains("WHITE")
        df.withColumn("has", containsBlack or containsWhite)
          .where("has")
          .select("Description").show(3, false)
    }

    test("varargs") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val colors = Seq("black", "white", "red", "green", "blue")

        val selectedColumns = colors.map(color => {
            col("Description").contains(color.toUpperCase()).as(s"is_$color")
        }) :+ expr("*")

        df.select(selectedColumns:_*)           // varargs를 통해 임의 길이 컬럼 배열을 핸들링 가능하다.
          .where(expr("is_white or is_red"))
          .show(3, false)
        /*
        +--------+--------+------+--------+-------+---------+---------+--------------------------------+--------+-------------------+---------+----------+--------------+
        |is_black|is_white|is_red|is_green|is_blue|InvoiceNo|StockCode|Description                     |Quantity|InvoiceDate        |UnitPrice|CustomerID|Country       |
        +--------+--------+------+--------+-------+---------+---------+--------------------------------+--------+-------------------+---------+----------+--------------+
        |false   |false   |true  |false   |false  |562127   |20832    |RED FLOCK LOVE HEART PHOTO FRAME|24      |2011-08-03 08:21:00|0.39     |13717.0   |United Kingdom|
        |false   |true    |false |false   |false  |562127   |84660A   |WHITE STITCHED WALL CLOCK       |20      |2011-08-03 08:21:00|0.79     |13717.0   |United Kingdom|
        |false   |false   |true  |false   |false  |562128   |22669    |RED BABY BUNTING                |5       |2011-08-03 09:07:00|2.95     |16150.0   |United Kingdom|
        +--------+--------+------+--------+-------+---------+---------+--------------------------------+--------+-------------------+---------+----------+--------------+
         */
    }
}
