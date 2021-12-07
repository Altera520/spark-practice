package chapter08

import common.CommonSpark
import org.apache.spark.sql.functions.expr
import org.scalatest.funsuite.AnyFunSuite

class ComplexDataTypeJoin extends AnyFunSuite {
    test ("complex_type_join") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        val (person, graduateProgram, sparkStatus) = JoinData.read(spark)

        person.show(1, false)
        /*
        +---+-------------+----------------+------------+
        |id |name         |graduate_program|spark_status|
        +---+-------------+----------------+------------+
        |0  |Bill Chambers|0               |[100]       |
        +---+-------------+----------------+------------+
         */

        sparkStatus.show(1, false)
        /*
        +---+--------------+
        |id |status        |
        +---+--------------+
        |500|Vice President|
        +---+--------------+
         */

        // person df의 spark_status 컬럼과 sparkStatus df의 id 컬럼을 조인하고자 함
        // person df와 sparkStatus df에 id 컬럼명이 겹치므로 person df의 id 컬럼을 조인전 rename
        person.withColumnRenamed("id", "psrsonId")
          .join(sparkStatus, expr("array_contains(spark_status, id)")).show()
        /*
        +--------+----------------+----------------+---------------+---+--------------+
        |psrsonId|            name|graduate_program|   spark_status| id|        status|
        +--------+----------------+----------------+---------------+---+--------------+
        |       0|   Bill Chambers|               0|          [100]|100|   Contributor|
        |       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
        |       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
        |       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
        |       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
        |       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
        +--------+----------------+----------------+---------------+---+--------------+
         */
    }
}
