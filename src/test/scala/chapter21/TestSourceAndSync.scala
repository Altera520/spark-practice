package chapter21

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

class TestSourceAndSync extends AnyFunSuite{
    val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
    import spark.implicits._

    test("socket_source") {
        // 스파크 애플리케이션을 실행하면 9999포트로 데이터를 전송할 수 있다.
        // nc -lk 9999
        spark.readStream
          .format("socket")
          .option("host", "localhost")
          .option("port", 9999)
          .load()

    }

    test("console_sync") {
        Seq("hello").toDS()
          .writeStream
          .format("console")
          .outputMode("complete")
          .start()
    }

    test("memory_sync") {
        Seq("hello").toDS()
          .writeStream
          .format("memory")
          .queryName("my_device_table")
    }
}
