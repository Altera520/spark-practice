package template.etl

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class TemplateEtlTest extends AnyFunSuite {

    test("TemplateEtl") {
        val spark = CommonSpark.createLocalSparkSession("test")
        System.setProperty("profile", "local")

        val df = spark.range(500).toDF("number")
        df.show()

        df.select(df.col("number") + 10).show()
    }

    test ("scala_test") {
        val x = Some("String")
        val res = x match {
            case Some(a) => a
            case _ => null
        }
        println(res)
    }
}
