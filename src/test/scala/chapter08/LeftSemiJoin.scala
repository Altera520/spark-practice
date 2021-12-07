package chapter08

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class LeftSemiJoin extends AnyFunSuite{
    test ("left_semi_join") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        import spark.implicits._
        val (person, graduateProgram, sparkStatus) = JoinData.read(spark)
        val joinExpr = person.col("graduate_program") === graduateProgram.col("id")

        person.show()
        /*
        +---+----------------+----------------+---------------+
        | id|            name|graduate_program|   spark_status|
        +---+----------------+----------------+---------------+
        |  0|   Bill Chambers|               0|          [100]|
        |  1|   Matei Zaharia|               1|[500, 250, 100]|
        |  2|Michael Armbrust|               1|     [250, 100]|
        +---+----------------+----------------+---------------+
         */

        graduateProgram.join(person, joinExpr, "left_semi").show()
        /*
        +---+-------+--------------------+-----------+
        | id| degree|          department|     school|
        +---+-------+--------------------+-----------+
        |  0|Masters|School of Informa...|UC Berkeley|
        |  1|   Ph.D|School of Informa...|UC Berkeley|
        +---+-------+--------------------+-----------+
         */

        val gradProgram2 = graduateProgram.union(
            Seq(
                (0, "Masters", "Duplicated Row", "Duplicated School")
            ).toDF()
        )

        // 왼쪽 DF의 row에 중복키가 존재하더라도 왼쪽 DF의 row는 결과에 포함된다.
        gradProgram2.join(person, joinExpr, "left_semi").show()
        /*
        +---+-------+--------------------+-----------------+
        | id| degree|          department|           school|
        +---+-------+--------------------+-----------------+
        |  0|Masters|School of Informa...|      UC Berkeley|
        |  1|   Ph.D|School of Informa...|      UC Berkeley|
        |  0|Masters|      Duplicated Row|Duplicated School|
        +---+-------+--------------------+-----------------+
         */
    }
}
