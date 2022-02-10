package chapter14

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class BroadCastVariable extends AnyFunSuite{
    test("broadcast") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
          .split(StringUtils.SPACE)

        // RDD 구성
        val words = spark.sparkContext.parallelize(myCollection, 2)

        // 단어 목록, 수 기가 바이트라 가정, 이 구조체를 스파크에 브로드캐스트
        // 브로드 캐스트된 불변 값은 액션이 실행될때 클러스터의 모든 노드에 지연처리 방식으로 복제
        val supplementalData = Map(
            "Spark" -> 1000,
            "Definitive" -> 200,
            "Big" -> 300,
            "Simple" -> 100
        )

        // 브로드캐스트 변수의 value 메서드를 사용해 supplementalData값을 참조 가능
        // value 메서드는 직렬화된 함수에서 브로드 캐스트된 데이터를 직렬화 하지 않아도 접근 가능
        // 브로드캐스트를 통해 직렬화와 역직렬화에 대한 부하를 크게 줄일 수
        val suppBroadcast = spark.sparkContext.broadcast(supplementalData)

        // PairRDD를 생성하고, key-value에서 value 기준으로 sort
        words.map { word =>
            (word, suppBroadcast.value.getOrElse(word, 0))
        }
          .sortBy(_._2)
          .collect() foreach(println)
    }
}
