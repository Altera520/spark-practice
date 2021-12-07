package chapter08

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class RightOuterJoin extends AnyFunSuite{
    test ("right_outer_join") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        val (person, graduateProgram, sparkStatus) = JoinData.read(spark)
        val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

        // 3번째 인자 joinType을 right outer로 지정한다.
        person.join(graduateProgram, joinExpr, "right_outer").show()
        /*
        +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
        |  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
        +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
        |   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
        |null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
        |   2|Michael Armbrust|               1|     [250, 100]|  1|   Ph.D|School of Informa...|UC Berkeley|
        |   1|   Matei Zaharia|               1|[500, 250, 100]|  1|   Ph.D|School of Informa...|UC Berkeley|
        +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
         */
    }
}
