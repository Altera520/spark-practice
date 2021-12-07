package chapter08

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class CrossJoin extends AnyFunSuite {
    test("cross_join") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        import spark.implicits._
        val (person, graduateProgram, sparkStatus) = JoinData.read(spark)
        val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

        graduateProgram.join(person, joinExpr, "cross").show()
        /*
        +---+-------+--------------------+-----------+---+----------------+----------------+---------------+
        | id| degree|          department|     school| id|            name|graduate_program|   spark_status|
        +---+-------+--------------------+-----------+---+----------------+----------------+---------------+
        |  0|Masters|School of Informa...|UC Berkeley|  0|   Bill Chambers|               0|          [100]|
        |  1|   Ph.D|School of Informa...|UC Berkeley|  2|Michael Armbrust|               1|     [250, 100]|
        |  1|   Ph.D|School of Informa...|UC Berkeley|  1|   Matei Zaharia|               1|[500, 250, 100]|
        +---+-------+--------------------+-----------+---+----------------+----------------+---------------+
         */

        // 명시적으로 메서드를 호출할 수도 있다.
        // 교차조인은 위험하다. 정말로 필요한 경우에만 교차 조인을 사용해야한다.
        person.crossJoin(graduateProgram).show()
    }
}
