package chapter02

import common.CommonSpark
import common.CommonUtil.{getAppName, watchTime}
import org.apache.spark.sql.functions.desc
import org.scalatest.funsuite.AnyFunSuite

class flight_data_Test extends AnyFunSuite {
    lazy val appName = getAppName(this)
    lazy val spark = CommonSpark.createLocalSparkSession(appName)

    test (s"${appName}_sort") {
        spark.conf.set("spark.sql.shuffle.partitions", 5)
        watchTime(appName) {
            val df = flight_data.read(spark)
            df.show(5)

            df.sort("count").explain()

            df.sort("count").take(2)
        }
    }

    test (s"${appName}_sql") {
        watchTime(appName) {
            flight_data.read(spark)
              .createOrReplaceTempView("flight_data_2015")  // DF를 테이블이나 뷰로 생성
            val sql_df = spark.sql(
                """
                  | select DEST_COUNTRY_NAME, count(1)
                  |   from flight_data_2015
                  |  group by DEST_COUNTRY_NAME
                  |""".stripMargin)

            val general_df = flight_data
              .read(spark)
              .groupBy("DEST_COUNTRY_NAME")
              .count()

            // DataFrame과 Spark Sql의 실행 플랜은 같다
            sql_df.explain()
            general_df.explain()
        }
    }

    test (s"${appName}_max_min") {
        import org.apache.spark.sql.functions.max
        watchTime(appName) {
            println {
                flight_data.read(spark)
                  .select(max("count"))
                  .take(1)
            }
        }
    }

    test (s"${appName}_multiple_transformation") {
        watchTime(appName) {
            flight_data.read(spark)
              .createOrReplaceTempView("flight_data_2015")

            val df = spark.sql(
                """
                  | select dest_country_name, sum(count) as dest_total
                  |   from flight_data_2015
                  |  group by dest_country_name
                  |  order by sum(count) desc
                  |  limit 5
                  |""".stripMargin)

            df.explain()

            df.show()
        }
    }

    test (s"${appName}_multiple_transformation_2nd") {
        watchTime(appName) {
            flight_data.read(spark)
              .groupBy("dest_country_name")
              .sum("count")
              .withColumnRenamed("sum(count)", "dest_total")
              .sort(desc("dest_total"))
              .limit(5)
              .show()
        }
    }
}
