package chapter08

import org.apache.spark.sql.SparkSession

object JoinData {
    def read(spark: SparkSession) = {
        import spark.implicits._

        val person = Seq(
            (0, "Bill Chambers", 0, Seq(100)),
            (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
            (2, "Michael Armbrust", 1, Seq(250, 100)))
          .toDF("id", "name", "graduate_program", "spark_status")
        person.createOrReplaceTempView("person")

        val graduateProgram = Seq(
            (0, "Masters", "School of Information", "UC Berkeley"),
            (2, "Masters", "EECS", "UC Berkeley"),
            (1, "Ph.D", "School of Information", "UC Berkeley"))
          .toDF("id", "degree", "department", "school")
        graduateProgram.createOrReplaceTempView("graduateProgram")

        val sparkStatus = Seq(
            (500, "Vice President"),
            (250, "PMC Member"),
            (100, "Contributor"))
          .toDF("id", "status")
        sparkStatus.createOrReplaceTempView("sparkStatus")

        (person, graduateProgram, sparkStatus)
    }
}
