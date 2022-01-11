package chapter06

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class JsonMul extends AnyFunSuite{
    test("json") {
        val spark = CommonSpark.createLocalSparkSession("test")

        val jsonDF = spark.range(1).selectExpr(
            """
              |'{"myJsonKey": {"myJsonValue": [1, 2, 3]}}' as jsonString
              |""".stripMargin)

        jsonDF.show(2, false)
        /*
        +-----------------------------------------+
        |jsonString                               |
        +-----------------------------------------+
        |{"myJsonKey": {"myJsonValue": [1, 2, 3]}}|
        +-----------------------------------------+
         */

        jsonDF.printSchema()
        /*
        root
        |-- jsonString: string (nullable = false)
         */

        jsonDF.select(
            // get_json_object 함수를 통해 JSON 문자열을 조회가능
            get_json_object(col("jsonString"), "$.myJsonKey.myJsonValue[1]") as "column",
            // 중첩이 없는 단일 수준의 JSON이라면 json_tuple을 사용하여 조회 가능
            json_tuple(col("jsonString"), "myJsonKey"),
        ).show(2, false)
        /*
        +------+-----------------------+
        |column|c0                     |
        +------+-----------------------+
        |2     |{"myJsonValue":[1,2,3]}|
        +------+-----------------------+
         */
    }

    test("to_json") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val df2 = df.selectExpr("(InvoiceNo, Description) as myStruct")
          .withColumn("json_obj", to_json(col("myStruct")))     // to_json을 통해 StructType을 JsonType으로 만들수 있다.
        df2.show(2, false)
        /*
        +------------------------------------------+-----------------------------------------------------------------------+
        |myStruct                                  |json_obj                                                               |
        +------------------------------------------+-----------------------------------------------------------------------+
        |{562127, GROOVY CACTUS INFLATABLE}        |{"InvoiceNo":"562127","Description":"GROOVY CACTUS INFLATABLE"}        |
        |{562127, RED FLOCK LOVE HEART PHOTO FRAME}|{"InvoiceNo":"562127","Description":"RED FLOCK LOVE HEART PHOTO FRAME"}|
        +------------------------------------------+-----------------------------------------------------------------------+
         */
        df2.printSchema()
        /*
        root
         |-- myStruct: struct (nullable = false)
         |    |-- InvoiceNo: string (nullable = true)
         |    |-- Description: string (nullable = true)
         |-- json_obj: string (nullable = true)
         */
    }

    test("from_json") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark.read
          .format("csv")
          .option("header", true) // csv에서 header를 컬럼명으로 지정하려면 옵션을 줘야한다.
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/retail-data/by-day/2011-08-03.csv")

        val parseSchema = StructType(
            Seq(
                StructField("InvoiceNo", StringType, true),
                StructField("Description", StringType, true)
            )
        )

        df.selectExpr("(InvoiceNo, Description) as myStruct")
          // struct type을 json 문자열로
          .select(to_json(col("myStruct")) as "newJson")
          // json 문자열을 struct type으로, schema 필요
          .select(from_json(col("newJson"), parseSchema), col("newjson"))
          .show(2, false)
        /*
        +------------------------------------------+-----------------------------------------------------------------------+
        |from_json(newJson)                        |newjson                                                                |
        +------------------------------------------+-----------------------------------------------------------------------+
        |{562127, GROOVY CACTUS INFLATABLE}        |{"InvoiceNo":"562127","Description":"GROOVY CACTUS INFLATABLE"}        |
        |{562127, RED FLOCK LOVE HEART PHOTO FRAME}|{"InvoiceNo":"562127","Description":"RED FLOCK LOVE HEART PHOTO FRAME"}|
        +------------------------------------------+-----------------------------------------------------------------------+
         */
    }
}
