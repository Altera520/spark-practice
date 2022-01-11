package chapter12

import common.CommonUtil.P
import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

class CreateRDD extends AnyFunSuite {
    test("rdd") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        import spark.implicits._

        // Dataset[Long]을 RDD[Long]으로 변환
        spark.range(100).rdd

        spark.range(100).toDF().rdd.map(rowObject => rowObject.getLong(0))

        // RDD[Long]을 DataFrame으로
        spark.range(100).rdd.toDF()
    }

    test("collection_to_rdd") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val ls = List(1, 2, 3)

        // 로컬 컬렉션으로 rdd 생성
        val rdd = spark.sparkContext.parallelize(ls, 3)

        // rdd에 이름을 지정하면 스파크 UI에 지정한 이름으로 RDD가 표시
        rdd.setName("words")
        println(rdd.name)
    }

    test("datasource_to_rdd") {
        // 데이터소스나 파일을 사용해 RDD를 만들수 있으나, DataSource API를 사용하는것이 맞음
        // RDD에는 DF가 제공하는 DataSource API가 존재 X
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        // 텍스트 파일의 각줄을 레코드로 가진 rdd
        val rdd = spark.sparkContext.textFile(s"${P.rawDataPath}/data/retail-data/by-day/2011-12-09.csv")

        // 전체 텍스트파일 하나가 레코드
        val whole_rdd = spark.sparkContext.wholeTextFiles(s"${P.rawDataPath}/data/retail-data/by-day/2011-12-09.csv")
    }
}
