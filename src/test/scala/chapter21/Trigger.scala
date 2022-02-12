package chapter21

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.streaming.Trigger
import org.scalatest.funsuite.AnyFunSuite

class Trigger extends AnyFunSuite{
    val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
    import spark.implicits._

    test("processing_time_trigger") {
        Seq("hello").toDS()
          .writeStream
          .trigger(Trigger.ProcessingTime("100 seconds"))
          .format("console")
          .outputMode("complete")
          .start()
    }

    test("trigger_once") {
        Seq("hello").toDS()
          .writeStream
          .trigger(Trigger.Once)
          .format("console")
          .outputMode("complete")
          .start()
    }
}
