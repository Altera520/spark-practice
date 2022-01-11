package chapter12

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class SendSystemCommand extends AnyFunSuite {
    test("pipe") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = "Hello World !".split(StringUtils.SPACE)
        val rdd = spark.sparkContext.parallelize(words,3)

        print(rdd.pipe("wc -l").count())
    }

    test("mapPartitions") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = "Hello World !".split(StringUtils.SPACE)
        val rdd = spark.sparkContext.parallelize(words, 3)

        // map함수에서 MapPartitionsRDD를 반환, map은 mapPartitions함수의 alias
        // 개별 파티션(이터레이터로 표현)에 대해 map 연산을 수행

        // 모든 파티션에 1 값을 생성하고 파티션 수를 세어 합산
        print(rdd.mapPartitions(part => Iterator[Int](1)).sum())
    }

    test("mapPartitionsWithIndex") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = "Hello World !".split(StringUtils.SPACE)
        val rdd = spark.sparkContext.parallelize(words, 3)

        /**
         *
         * @param partitionIndex        RDD의 파티션 번호, 디버깅에 활용 가능
         * @param withinPartIterator    파티션의 모든 아이템을 순회가능한 이터레이터
         * @return
         */
        def indexedFunc(partitionIndex: Int, withinPartIterator: Iterator[String]) = {
            withinPartIterator.toList.map {
                value => s"Partition: $partitionIndex => $value"
            }.iterator
        }

        rdd.mapPartitionsWithIndex(indexedFunc).collect()
    }

    test("foreachPartition") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = "Hello World !".split(StringUtils.SPACE)
        val rdd = spark.sparkContext.parallelize(words, 3)

        rdd.foreachPartition { iter =>
            import java.io._
            import scala.util.Random

            val randomFileName = new Random().nextInt()
            val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))

            while (iter.hasNext) {
                pw.write(iter.next())
            }
            pw.close()
        }
    }

    test("glom") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        // 입력된 단어를 두개의 파티션에 개별적으로 할당
        spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()
        // Array(Array(Hello), Array(World))
    }
}
