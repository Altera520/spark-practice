package chapter13

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.SparkSession

object Prepare {
    def generateDataset(spark: SparkSession) = {
        val words = spark.sparkContext.parallelize(
            "Hello World Spark".split(StringUtils.SPACE),
            3)

        val chars = words.flatMap(word => word.toLowerCase().toSeq)
        val keyValueChars = chars.map(_ -> 1)
        keyValueChars
    }

    def maxFunc(left: Int, right: Int) = math.max(left, right)

    def addFunc(left: Int, right: Int) = left + right

    def nums(spark: SparkSession) = spark.sparkContext.parallelize(1 to 30, 5)
}
