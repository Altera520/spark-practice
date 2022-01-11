package chapter12

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class TransformationRDD extends AnyFunSuite {
    test("distinct") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val ls = List(1, 2, 3, 3, 2, 4)

        // 로컬 컬렉션으로 rdd 생성
        val num = spark.sparkContext.parallelize(ls, 3)

        println(num.distinct().count())
    }

    test("filter") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        // 로컬 컬렉션으로 rdd 생성
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        val startWithS = (individual: String) => individual.startsWith("S")

        words.filter(startWithS(_)).collect()
    }

    test("map") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        // 로컬 컬렉션으로 rdd 생성
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        val words2 = words.map(word => (word, word(0), word.startsWith("S")))
        words2.filter(r => r._3).take(5)
    }

    test("flatMap") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        // 로컬 컬렉션으로 rdd 생성
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        val words2 = words.flatMap(_ toSeq)
        words2.foreach(println(_))
    }

    test("sortBy") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        // 로컬 컬렉션으로 rdd 생성
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        words.sortBy(word => word.length * -1) // desc
    }

    test("randomSplit") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        // 로컬 컬렉션으로 rdd 생성
        val words = spark.sparkContext.parallelize(
            "Start with Hello".split(StringUtils.SPACE),
            3
        )
        val fiftyFiftySplit = words.randomSplit(Array[Double](0.5, 0.5))
    }
}
