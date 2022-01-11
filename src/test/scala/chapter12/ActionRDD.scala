package chapter12

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class ActionRDD extends AnyFunSuite {
    test("reduce") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        spark.sparkContext.parallelize(1 to 20).reduce(_ + _)

        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        val wordLengthReducer = (leftWord: String, rightWord: String) => {
            if (leftWord.length > rightWord.length) {
                leftWord
            }
            else {
                rightWord
            }
        }
        words.reduce(wordLengthReducer)
        // 파티션에 대한 리듀스 연산은 비결정적인 특성
        // leftWord에 들어오는 값이 항상 똑같은 순서로 들어오는 것이 아님
        // reduce 메서드를 실행할때마다 다른 결과를 반환할 수 있다.
    }

    test("count") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        words.count()
    }

    test("countApprox") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )

        val confidence = 0.95               // 신뢰도, 실제로 연산한 결과와의 오차율, 실제 연산과 동일한 값이 95%이상으로 포함될것을 기대가능 (0 ~ 1)
        val timeoutMilliseconds = 400       // 제한시간, 제한시간을 초과하면 불완전한 결과 반환할수
        words.countApprox(timeoutMilliseconds, confidence)
    }

    test("countApproxDistinct") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        /*
        첫번째 구현체
        상대정확도를 인자로전달, 이 값을 적게 설정하면 더 많은 메모리 공간 사용하는 카운터 생성
        값은 0.000017보다 커야
         */
        words.countApproxDistinct(0.05)

        /*
        두번째 구현체
        동작을 세부적으로 제어, 상대정확도를 지정할때 두개의 인자 지정(p: 정밀도, sp: 희소정밀도)
        카디널리티가 작을때 sp > p를 설정하면 메모리 소비 줄이면서 정확도 증가 가능
         */
        words.countApproxDistinct(4, 10)
    }

    test("countByValue") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        // RDD값의 갯수 구하는 메서드
        // 익스큐터의 연산 결과가 드라이버 메모리에 모두 적재 - 결과가 작은 경우에만 사용해야함
        words.countByValue()
    }

    test("countByValueApprox") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        val confidence = 0.95               // 신뢰도, 실제로 연산한 결과와의 오차율, 실제 연산과 동일한 값이 95%이상으로 포함될것을 기대가능 (0 ~ 1)
        val timeoutMilliseconds = 400       // 제한시간, 제한시간을 초과하면 불완전한 결과 반환할수
        words.countByValueApprox(timeoutMilliseconds, confidence)
    }

    test("first") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE)
        )
        // 첫번째값 반환
        words.first()
    }

    test("max_min") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        println(spark.sparkContext.parallelize(1 to 20).max())
        println(spark.sparkContext.parallelize(1 to 20).min())
    }

    test("take") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val plain = "Start with data".split(StringUtils.SPACE)
        val words = spark.sparkContext.parallelize(plain)

        // 먼저 하나의 파티션을 스캔
        // 해당 파티션의 결과 수를 이용해 파라미터로 지정된 값을 만족하는데 필요한 추가 파티션수 예측
        words.take(5)

        // take의 유사함수
        words.takeOrdered(5)
        words.top(5)            // 암시적 순서에따라 최상위값을 선택

        // 고정크기의 임의표본 데이터를 얻어오는 방법
        val withReplacement = true
        val numberToTake = 6            // 임의 표본수
        val randomSeed = 100L           // 난수 시드
        words.takeSample(withReplacement, numberToTake, randomSeed)
    }
}
