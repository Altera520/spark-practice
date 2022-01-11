package chapter12

import common.{CommonSpark, CommonUtil}
import org.apache.hadoop.io.compress.BZip2Codec
import org.scalatest.funsuite.AnyFunSuite

class FileWrite extends AnyFunSuite {
    test("saveAsTextFile") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val nums = spark.sparkContext.parallelize(Seq(1, 2, 3))

        // 경로 지정하여 텍스트 파일로 저장
        nums.saveAsTextFile("file:///tmp/bookTitle")

        // 파일저장시 압축코덱지정하여 저장
        nums.saveAsTextFile("file:///tmp/bookTitleCompressed", classOf[BZip2Codec])
    }

    test("saveAsObjectFile") {
        // 시퀀스 파일(key-value pair로 구성된 파일) - MR의 입출력 포맷
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val nums = spark.sparkContext.parallelize(Seq(1, 2, 3))

        // 시퀀스파일로 저장
        nums.saveAsObjectFile("/tmp/my/sequenceFilePath")
    }
}
