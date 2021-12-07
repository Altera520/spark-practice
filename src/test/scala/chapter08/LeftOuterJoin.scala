package chapter08

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class LeftOuterJoin extends AnyFunSuite{
    test("left_outer_join") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        val (person, graduateProgram, sparkStatus) = JoinData.read(spark)

        val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

        // 3번째 인자 joinType을 left outer로 지정한다.
        graduateProgram.join(person, joinExpr, "left_outer").show()
        /*
        +---+-------+--------------------+-----------+----+----------------+----------------+---------------+
        | id| degree|          department|     school|  id|            name|graduate_program|   spark_status|
        +---+-------+--------------------+-----------+----+----------------+----------------+---------------+
        |  0|Masters|School of Informa...|UC Berkeley|   0|   Bill Chambers|               0|          [100]|
        |  2|Masters|                EECS|UC Berkeley|null|            null|            null|           null|
        |  1|   Ph.D|School of Informa...|UC Berkeley|   2|Michael Armbrust|               1|     [250, 100]|
        |  1|   Ph.D|School of Informa...|UC Berkeley|   1|   Matei Zaharia|               1|[500, 250, 100]|
        +---+-------+--------------------+-----------+----+----------------+----------------+---------------+
         */
    }
}
