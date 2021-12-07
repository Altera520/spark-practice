package chapter09

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class CsvFileRead extends AnyFunSuite {
    test ("csv_file_read_failfast") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val manualSchema = StructType(
            Seq(
                StructField("dest_country_name", StringType, true),
                StructField("origin_country_name", StringType, true),
                StructField("count", LongType, false),
            )
        )
        spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("mode", "FAILFAST")
          .schema(manualSchema)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/csv/2010-summary.csv")
          .show(3, false)
    }

    test ("csv_file_read_failfast_lazy_error") {
        val spark = CommonSpark.createLocalSparkSession("test")

        // 데이터 포맷이랑 맞지 않는 스키마 정의
        val manualSchema = StructType(
            Seq(
                StructField("dest_country_name", LongType, true),
                StructField("origin_country_name", LongType, true),
                StructField("count", LongType, false),
            )
        )

        // df 정의 시점에는 오류가 발생하지 않는다.
        val df = spark.read
          .format("csv")
          .option("header", true)
          .option("mode", "FAILFAST") // 데이터 포맷이랑 맞지 않는 스키마 적용
          .schema(manualSchema)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/csv/2010-summary.csv")

        // 오류는 액션을 취하는 시점에 발생(잡 실행 시점), 스파크는 지연 연산 특성이 있으므로
        df.take(5)
    }
}
