package common

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

object CommonUtil extends LazyLogging{

    lazy val config: Config = {
        val cfg = ConfigFactory.parseFile(new File("conf", "application-local.conf"))
        cfg
    }

    lazy val P = AppConfig(config)

    /**
     * block 실행하고 실행시간 출력
     * @param name
     * @param block
     * @tparam T
     * @return
     */
    def watchTime[T](name: String)(block: => T): T = {
        val start = System.currentTimeMillis()
        val ret = block
        val end = System.currentTimeMillis()
        val elapsed = (end - start) / 1000
        println(s"[success] $name time: ${elapsed} sec.")
        ret
    }

    def getDummyDataframe(spark: SparkSession): DataFrame = {
        spark.range(1000).toDF("number")
    }

    def getAppName[T](`class`: T) = `class`.getClass.getName.replace("$", "")
}
