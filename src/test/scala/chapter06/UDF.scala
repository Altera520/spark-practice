package chapter06

import common.CommonSpark
import org.apache.spark.sql.functions.{col, udf}
import org.scalatest.funsuite.AnyFunSuite

class UDF extends AnyFunSuite{
    test ("udf") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val udfExampleDF = spark.range(5).toDF("num")
        val power3 = (num: Double) => num * num * num

        // DataFrame에서 사용할 수 있게 udf에 함수 등록, 스파크 sql(expr)에서는 사용 X
        val power3Udf = udf(power3(_: Double): Double)

        // DF에서 사용
        udfExampleDF.select(power3Udf(col("num"))).show(2)

        // UDF를 spark sql 함수로 등록하면 모든 프로그래밍 언어와 SQL에서 사용자 정의함수 사용가능
        // 이렇게 등록하면 udf를 sql 표현식으로만 사용할 수 있음, DataFrame 함수로는 사용 X
        spark.udf.register("power3", power3(_:Double): Double)          // 함수의 반환 타입을 명시하는 것이 좋다. 지정된 반환타입이 아니면 null이 반환된다.
        udfExampleDF.selectExpr("power3(num)").show(2)
    }
}
