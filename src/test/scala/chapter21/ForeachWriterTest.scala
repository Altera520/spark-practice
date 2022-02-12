package chapter21

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.ForeachWriter

class ForeachWriterTest extends AnyFunSuite{
    test("") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        import spark.implicits._

        val ds = Seq("hello").toDS()

        ds.writeStream
          .foreach(new ForeachWriter[String] {
              // 처리하려는 row를 식별가능한 id값이 파라미터
              override def open(partitionId: Long, epochId: Long): Boolean = {
                  // 연결 생성
                  // row의 처리여부를 true or false로 반환해야
                  true
              }

              override def process(value: String): Unit = {
                  // open 메서드가 true를 반환하면 process는 데이터의 레코드마다 호출
                  // 데이터를 처리하거나 저장하는 용도로만 사용
              }

              override def close(errorOrNull: Throwable): Unit = {
                  // open 메서드의 반환값 여부와 관계없이 open 호출되면 이후 close도 호출됨
                  // 스트림 처리중 오류 발생시 close에서 오류를 받게됨
                  // 열려있는 자원 해체 작업을 수행시켜야
              }
          })
    }
}
