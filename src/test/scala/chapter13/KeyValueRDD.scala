package chapter13

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class KeyValueRDD extends AnyFunSuite {
    test("pairRDD") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello World".split(StringUtils.SPACE), 2)

        // map에서 tuple을 반환하여 PairRDD 생성
        val pair = words.map(_.toLowerCase -> 1)

        println(pair.getClass.getTypeName)
    }

    test("keyBy") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello World".split(StringUtils.SPACE), 2)

        // 단어의 첫번째 문자를 key, 원래 단어는 value로 하여 RDD 생성
        val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)
    }

    test("mapValues") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello World".split(StringUtils.SPACE), 2)
        val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

        // key,value pair에서 value만 추출하여 값 수정
        keyword.mapValues(_.toUpperCase()).collect() foreach {
            item => println(item)
        }
        /*
        [(h,HELLO), (w,WORLD)]
         */
    }

    test("key_value_extract") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello World".split(StringUtils.SPACE), 2)
        val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

        // key
        keyword.keys.collect() foreach { key =>
            println(key)
        }

        // value
        keyword.values.collect() foreach { value =>
            println(value)
        }
    }

    test("lookup") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Spark simple say".split(StringUtils.SPACE), 2)
        val keyword = words.keyBy(word => word.toLowerCase.toSeq(0).toString)

        keyword.lookup("s") foreach { item =>
            println(item)
        }
        /*
        Spark
        simple
        say
         */
    }

    test("sampleByKey") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Spark simple say".split(StringUtils.SPACE), 2)

        val distinctChars = words.flatMap(word => word.toLowerCase.toSeq).distinct.collect()
        // s, p, a, r, k, i, m, l, e, y

        import scala.util.Random
        // Tuple[Double, Char] 형식의 PairRDD 생성
        val sampleMap = distinctChars.map(_ -> new Random().nextDouble()).toMap

        words.map(word => word.toLowerCase.toSeq(0) -> word)
          // RDD를 한번만 처리하면서 간단한 무작위 샘플링을 사용
          // math.ceil(numItems * samplingRate) 값의 총합과 거의 동일한 크기의 샘플 생성
          .sampleByKey(true, sampleMap, 6L)
          .collect()

        words.map(word => word.toLowerCase.toSeq(0) -> word)
          // math.ceil(numItems * samplingRate)의 합과 동일한 샘플 데이터 생성
          .sampleByKeyExact(true, sampleMap, 6L)
          .collect()
    }
}
