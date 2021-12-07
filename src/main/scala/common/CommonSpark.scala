package common

import com.typesafe.scalalogging.LazyLogging
import common.CommonUtil.P
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CommonSpark extends LazyLogging{

    lazy val conf = {
        val conf = new SparkConf()
        P.sparkOptions.foreach {
            case (k, v) => conf.set(k, v)
        }
        conf
    }

    /**
     * Yarn 환경 SparkSession 생성 - production
     * @param name
     * @return
     */
    def createYarnSparkSession(name: String) = {
        val spark = SparkSession
          .builder()
          .appName(name)
          .config(conf)
          //.enableHiveSupport()
          .getOrCreate()
        spark
    }

    /**
     * Local 환경 SparkSession 생성 - development
     * @param name
     * @return
     */
    def createLocalSparkSession(name: String) = {
        val spark = SparkSession
          .builder()
          .appName(name)
          .master("local")
          .config(conf)
          .config("spark.sql.shuffle.patitions", 5)  // shuffle시 기본적으로 200개의 파티션 생성, 로컬에서는 많은 익스큐터 필요X
          //.enableHiveSupport()
          .getOrCreate()
        spark
    }
}
