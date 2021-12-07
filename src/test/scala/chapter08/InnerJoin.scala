package chapter08

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class InnerJoin extends AnyFunSuite{

    // 교집합
    test("inner_join") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        val (person, graduateProgram, sparkStatus) = JoinData.read(spark)

        val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
        person.join(graduateProgram, joinExpr).show()
        /*
        +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
        | id|            name|graduate_program|   spark_status| id| degree|          department|     school|
        +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
        |  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
        |  2|Michael Armbrust|               1|     [250, 100]|  1|   Ph.D|School of Informa...|UC Berkeley|
        |  1|   Matei Zaharia|               1|[500, 250, 100]|  1|   Ph.D|School of Informa...|UC Berkeley|
        +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
         */

        // join 메서드의 세 번째 인자를 통해 조인 타입을 보다 명시적으로 지정가능
        person.join(graduateProgram, joinExpr, "inner").show()
    }
}
