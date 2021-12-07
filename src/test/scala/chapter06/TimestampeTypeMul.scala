package chapter06

import common.CommonSpark
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class TimestampeTypeMul extends AnyFunSuite {

    test("timestamp_func") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val df = spark.range(10)
          .withColumn("today", current_date())
          .withColumn("now", current_timestamp())
        df.createOrReplaceTempView("dateTable")

        df.printSchema()
        df.show(false)

        // today 기준 5일전 5일후
        df.select(date_sub(col("today"), 5), date_add(col("today"), 5))
          .show(1, false)

        // datediff
        df.withColumn("week_ago", date_sub(col("today"), 7))
          .select(datediff(col("week_ago"), col("today")))
          .show(1)

        // to_date는 문자열을 날짜로 변환, 날짜포맷도 지정가능
        // 날짜 포맷은 자바의 SimpleDateFormat 클래스가 지원하는 포맷을 사용해야
        df.select(
            to_date(lit("2016-01-01")).alias("start"),
            to_date(lit("2017-05-22")).alias("end")
        ).select(
            months_between(col("start"), col("end"))
        ).show(1)
    }

    // 날짜를 파싱할수 없으면 null이된다 -> 디버깅하기가 매우 어렵다.
    test("date_null") {
        val spark = CommonSpark.createLocalSparkSession("test")

        // yyyyddMM형식으로 데이터가 주어짐 -> 파싱할수 없으면 null이됨
        val df = spark.range(10)
          .select(to_date(lit("2016-20-12")), to_date(lit("2017-12-11")))
          .show(1, false)

        // dateFormat을 지정하여 문자열 리터럴 파싱처리
        val dateFormat = "yyyy-dd-MM"
        val cleanDateDF = spark.range(1).select(
            to_date(lit("2016-20-12"), dateFormat).alias("date"),
            to_date(lit("2017-12-11"), dateFormat).as("date2")
        )
        cleanDateDF.createOrReplaceTempView("dateTable2")

        spark.sql(
            """
              |select * from dateTable2
              |""".stripMargin)
          .show(false)
    }

    test("to_timestamp") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val dateFormat = "yyyy-MM-dd"

        val df = spark.range(1).select(
            to_timestamp(lit("2017-05-11"), dateFormat).as("date")
        )
        df.show(false)

        df.filter(col("date") > lit("2017-05-01")).show()
    }
}
