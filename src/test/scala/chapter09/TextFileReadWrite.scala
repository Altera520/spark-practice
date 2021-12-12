package chapter09

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

class TextFileReadWrite extends AnyFunSuite {
    test("text_read") {
        val spark = CommonSpark.createLocalSparkSession("test")
        spark.read
          .textFile(s"${CommonUtil.P.rawDataPath}/data/flight-data/csv/2010-summary.csv")
          .selectExpr("split(value, ',') as rows")
          .show(3)
        /*
        +--------------------+
        |                rows|
        +--------------------+
        |[DEST_COUNTRY_NAM...|
        |[United States, R...|
        |[United States, I...|
        +--------------------+
         */
    }

    test("text_write") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq(
            ("a")
        ).toDF("str")

        df.write
          .text("/tmp/simple-text-file.txt")
    }

    test("text_write_partition") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq(
            ("a", 1)
        ).toDF("str", "num")

        // 텍스트 파일에 데이터를 저장할 때 파티셔닝 작업을 수행하면 더 많은 컬럼을 저장 가능
        // 모든 파일에 컬럼을 추가하는 것이 아닌, 텍스트 파일이 저장되는 디렉터리에 폴더 별로 컬럼 저장
        df.write
          .partitionBy("num")               // num=1이라는 디렉토리가 simple-text-file.txt 디렉토리 하위에 생성
          .text("/tmp/simple-text-file.txt")
    }
}
