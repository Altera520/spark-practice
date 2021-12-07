package chapter08

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class LeftAntiJoin extends AnyFunSuite {
    test ("left_anti_join") {
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

        graduateProgram.join(person, joinExpr, "left_anti").show()
        /*
        +---+-------+----------+-----------+
        | id| degree|department|     school|
        +---+-------+----------+-----------+
        |  2|Masters|      EECS|UC Berkeley|
        +---+-------+----------+-----------+
         */
    }
}
