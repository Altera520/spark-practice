package chapter13

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

class JoinRDD extends AnyFunSuite {
    test("inner_join") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello world spark".split(StringUtils.SPACE), 3)
        val distinctChars = words.flatMap(_.toLowerCase.toSeq).distinct

        val keyedChars = distinctChars.map(c => (c, new Random().nextDouble()))
        val outputPartitions = 10

        Prepare.generateDataset(spark)
          .join(keyedChars)
          .count()

        Prepare.generateDataset(spark)
          // join하면서 출력파티션수 설정
          .join(keyedChars, outputPartitions)
          .count()
    }

    test("zip") {
        // zip은 진짜 조인은 아니지만, 두 개의 RDD를 결합하므로 조인이라 볼 수
        // 두 개의 RDD를 zip하여 연결하여 PairRDD를 생성
        // 두 개의 RDD는 동일한 수의 요소와 동일한 수의 파티션을 가져야한다.
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val numRange = spark.sparkContext.parallelize(0 to 9, 2)

        val rawWords = spark.sparkContext.parallelize("Hello world".split(StringUtils.SPACE), 2)
        val words = rawWords.flatMap(item => item.toLowerCase().toSeq)

        words.zip(numRange).collect() foreach { item =>
            println(item)
        }
        /*
        (h,0), (e,1), (l,2), (l,3), (o,4), (w,5), (o,6), (r,7), (l,8), (d,9)
         */
    }
}
