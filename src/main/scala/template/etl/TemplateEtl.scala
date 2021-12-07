package template.etl

import com.typesafe.scalalogging.LazyLogging
import common.CommonSpark
import common.CommonUtil.{getDummyDataframe, watchTime}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TemplateEtl extends LazyLogging {

    def appName = this.getClass.getName.replace("$", "")

    def read(spark: SparkSession) = {
        getDummyDataframe(spark)
    }

    def processing(df: DataFrame) = {
        df
    }

    def write(df: DataFrame) = {
        null
    }

    def main(args: Array[String]): Unit = {
        println(s"Usage: $appName [all]") // all -> 초기적재

        val isAll = if (args.length == 1) args(0) == "all" else false

        val spark = CommonSpark.createLocalSparkSession(appName)
        spark.conf.set("isAll", isAll)
        spark.conf.set("srcTable", s"${}.test_tbl1")
        spark.conf.set("dstTable", s"${}.test_tbl2")

        watchTime(appName) {
            val pipeline = read _ andThen processing andThen write
            pipeline(spark)
        }

        spark.stop()
    }
}
