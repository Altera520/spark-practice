package chapter06

import common.{AppConfig, CommonSpark, CommonUtil}
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

import scala.language.postfixOps

class DataTypeMul extends AnyFunSuite {

    test("lit") {
        val spark = CommonSpark.createLocalSparkSession("test")
        System.setProperty("profile", "local")

        val df = spark.emptyDataFrame
        df.select(lit(5), lit("five"), lit(5.0))
    }

    test("boolean") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.where(col("InvoiceNo").equalTo(562127))
          .select("InvoiceNo", "Description")
          .show(5, false)

        df
          .select("InvoiceNo", "Description")
          .where(col("InvoiceNo").equalTo(562127))
          .show(5, false)

        df
          .select("InvoiceNo", "Description")
          .where(col("InvoiceNo") === lit(562127))
          .show(5, false)

        df
          .select("InvoiceNo", "Description")
          .where(expr("InvoiceNo = 562127"))
          .show(5, false)

        df
          .select("InvoiceNo", "Description")
          .where(expr("InvoiceNo == 562127"))
          .show(5, false)

        df
          .select("InvoiceNo", "Description")
          .where(expr("InvoiceNo <> 562127"))
          .show(5, false)

        df
          .select("InvoiceNo", "Description")
          .where(col("InvoiceNo").notEqual(562127))
          .show(5, false)
    }

    test("and_or") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val priceFilter = col("UnitPrice") > 600
        val descripFilter = col("Description").contains("POSTAGE")
        val dotCodeFilter = col("StockCode") === "DOT"

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.where(col("StockCode").isin("DOT"))
          .where(priceFilter.or(descripFilter)) //or 구문을 사용하면 반드시 동일한 구문에 조건을 정의해야한다.
          .show()

        df.withColumn("isExpensive", dotCodeFilter.and(priceFilter.or(descripFilter)))
          .where("isExpensive")
          .select("UnitPrice", "isExpensive").show(5)
    }

    test("boolean with sql") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.withColumn("isExpensive", expr("not UnitPrice <= 250"))
          .where("isExpensive")
          .select("Description", "UnitPrice").show(5)
    }

    test("null") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.where(col("Description").eqNullSafe("hello")).show()
    }

    test("numeric_pow") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val quantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5
        df.select(col("CustomerID").as("custo"), quantity).show(2)
    }

    test("round_bround") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        // round의 정밀도를 조정해서 반올림할수 있다. 기본적으로는 소수점 값이 정확히 중간값 이상이라면 반올림
        df.select(round(col("UnitPrice"), 1), bround(col("UnitPrice"))).show(5)

        df.select(round(lit(2.5)), bround(lit(2.5))).show(2)
    }

    test("stat_monotically_increasing_id") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        df.select(monotonically_increasing_id()).show(2)

    }
}
