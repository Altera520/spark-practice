package chapter14

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

case class Flight(DEST_COUNTRY_NAME: String,
                  ORIGIN_COUNTRY_NAME: String,
                  count: BigInt)

class Accumulator extends AnyFunSuite {
    test("accumulator") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        import spark.implicits._

        val flights = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight/parquet/2010-summary.parquet")
          .as[Flight]

        import org.apache.spark.util.LongAccumulator

        // 이름이 지정된 어큐뮬레이터 생성하고 등록
        val accChina = new LongAccumulator
        spark.sparkContext.register(accChina, "China")

        // sparkContext를 사용해 어큐뮬레이터를 생성
        // val accChina2 = spark.sparkContext.longAccumulator("China")

        // 어큐뮬레이터에 값을 더하는 방법을 지정
        def accChinaFunc(flight_row: Flight) = {
            val destination = flight_row.DEST_COUNTRY_NAME
            val origin = flight_row.ORIGIN_COUNTRY_NAME

            // 아래 조건이 만족되면 해당 row의 count 값을 accunulator에 더한다
            if (destination == "China") {
                accChina.add(flight_row.count.toLong)
            }
            if (origin == "China") {
                accChina.add(flight_row.count.toLong)
            }
        }

        // Dataset의 각 row마다 연산을 수행한다
        flights.foreach(r => accChinaFunc(r))

        // 어큐뮬레이터 값을 프로그래밍 방식으로 조회
        // 이름이 지정됬다면 UI에서도 확인 가능
        println(accChina.value)
    }

    test("user_defined_accumulator") {
        import scala.collection.mutable.ArrayBuffer
        import org.apache.spark.util.AccumulatorV2

        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        import spark.implicits._

        val arr = ArrayBuffer[BigInt]()
        val flights = spark.read
          .parquet(s"${CommonUtil.P.rawDataPath}/data/flight-data/parquet/2010-summary.parquet")
          .as[Flight]

        // 사용자 정의 어큐뮬레이터
        class UserEvenAccumulator extends AccumulatorV2[BigInt, BigInt] {
            private var num: BigInt = 0

            override def isZero: Boolean = this.num == 0

            override def copy(): AccumulatorV2[BigInt, BigInt] = new UserEvenAccumulator

            override def reset(): Unit = this.num = 0

            override def add(v: BigInt): Unit = {
                if (v % 2 == 0) {
                    this.num += v
                }
            }

            override def merge(other: AccumulatorV2[BigInt, BigInt]): Unit = this.num += other.value

            override def value: BigInt = this.num
        }

        // 어큐뮬레이터 생성 및 등록
        val acc = new UserEvenAccumulator
        val newAcc = spark.sparkContext.register(acc, "evenAcc")

        // foreach 메서드는 액션, 스파크는 액션에서만 어큐뮬레이터 실행 보장
        // 각 row마다 함수를 한번씩 적용해 어큐뮬레이터 값을 증가
        println(acc.value)
        flights.foreach(r => acc.add(r.count))
        println(acc.value)
    }
}
