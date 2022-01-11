package chapter06

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class ComplexTypeMul extends AnyFunSuite{
    test("struct") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.selectExpr("(Description, InvoiceNo) as complex").show(false)

        val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
        complexDF.show(false)

        // .을 사용하거나 getField 메서드를 통해 내부에 접근
        complexDF.select("complex.Description")
        complexDF.select(col("complex").getField("Description"))

        // *를 사용하면 DataFrame 최상위 수준으로 끌어올리는것이 가능
        complexDF.select("complex.*").show(false)
    }

    test("array") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        // split하면 배열로 변환됨
        df.select(split(col("Description"), " ")).show(2)

        df.select(split(col("Description"), " ").alias("array_col"))
          .selectExpr("array_col[0]").show(2)

        // size를 통해 array의 길이를 알아내는 것이 가능
        df.select(functions.size(split(col("Description"), " "))).show(2)
    }

    test("array_contains") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        // array_contains를 통해 배열에 특정 값이 포함되어있는지 여부를 식별가능
        df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
    }

    test("explode") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        // explode는 배열 타입의 컬럼을 인자로 받아서 배열의 모든 값을 Row로 반환, 나머지 컬럼 값들은 Row마다 중복되어 표시
        df.withColumn("splitted", split(col("Description"), " "))
          .withColumn("exploded", explode(col("splitted")))
          .select("Description", "InvoiceNo", "exploded")
          .show(4, false)
        /*
        +--------------------------------+---------+----------+
        |Description                     |InvoiceNo|exploded  |
        +--------------------------------+---------+----------+
        |GROOVY CACTUS INFLATABLE        |562127   |GROOVY    |
        |GROOVY CACTUS INFLATABLE        |562127   |CACTUS    |
        |GROOVY CACTUS INFLATABLE        |562127   |INFLATABLE|
        |RED FLOCK LOVE HEART PHOTO FRAME|562127   |RED       |
        +--------------------------------+---------+----------+
        */
    }

    test("map") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val map_df = df.withColumn("complex_map", functions.map(col("Description"), col("InvoiceNo")).alias("complex_map"))

        map_df.select("complex_map").show(2, false)
        /*
        +--------------------------------------------+
        |complex_map                                 |
        +--------------------------------------------+
        |{GROOVY CACTUS INFLATABLE -> 562127}        |
        |{RED FLOCK LOVE HEART PHOTO FRAME -> 562127}|
        +--------------------------------------------+
        */

        map_df.selectExpr("complex_map['WHITE METAL LANTERN']").show(2, false)
        /*
        +--------------------------------+
        |complex_map[WHITE METAL LANTERN]|
        +--------------------------------+
        |null                            |
        |null                            |
        +--------------------------------+
        */

        map_df.select(explode(col("complex_map"))).show(2, false)
        /*
        +--------------------------------+------+
        |key                             |value |
        +--------------------------------+------+
        |GROOVY CACTUS INFLATABLE        |562127|
        |RED FLOCK LOVE HEART PHOTO FRAME|562127|
        +--------------------------------+------+
        */
    }

    test("string_array_explode") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._
        val df = Seq(
            (
              """
                |{"dtc": "1g1c;2g2c;3g3c"}
                |""".stripMargin)
        ).toDF("payload")
        df.show(false)

        df.withColumn(
            "dtc_raw", get_json_object(col("payload"), "$.dtc"))
          .withColumn("dtc", explode(split($"dtc_raw", ";")))
          .show(false)
    }
}
