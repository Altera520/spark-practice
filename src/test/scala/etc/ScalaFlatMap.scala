package etc

import common.CommonSpark
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class ScalaFlatMap extends AnyFunSuite {
    test("scala_map_test") {
        val ls = List("taewon 45", "minsu 6", "sunny 40")

        // val ls = List("taewan 45", "minsu 6", "sunny secret")
        // 위와같이 예상치 못한 패턴이 들어오면 NumberFormatException이 발생한다. "sunny secret"때문
        val normalizedList = ls.map { v =>
            val Array(name, age) = v.split(StringUtils.SPACE)
            (name, age.toInt)
        }

        normalizedList foreach {
            println(_)
        }
        /*
        (taewon,45)
        (minsu,6)
        (sunny,40)
         */
    }

    test("scala_flatmap_test") {
        val ls = List("taewan 45", "minsu 6", "sunny secret")

        val normalizedList = ls.map { v =>
            val Array(name, age) = v.split(StringUtils.SPACE)
            try {
                Some(name, age.toInt)
            } catch {
                case _: NumberFormatException => None
            }
        }
        println(normalizedList.mkString(","))
        // Some((taewan,45)),Some((minsu,6)),None

        val normalizedFlatList = ls.flatMap { v =>
            val Array(name, age) = v.split(StringUtils.SPACE)
            try {
                Some(name, age.toInt)
            } catch {
                case _: NumberFormatException => None
            }
        }
        println(normalizedFlatList.mkString(","))
        // (taewan,45),(minsu,6)
        // flatMap을 Option타입 엘리먼트에게 적용시 Some을 풀어버리고, None은 필터링
        // 1. map 함수 처럼 컬렉션 생성
        // 2. 랩핑된 객체 제거

        /*
        flatMap은 차원을 낮춘다.
        List, Array와 같은 컬렉션 또는 Some과 같이 어떤 객체를 감싸는 최상위 객체를 제거하고 그 안의 요소를 반환하여 컬렉션을 만드는 기능
         */
    }
}
