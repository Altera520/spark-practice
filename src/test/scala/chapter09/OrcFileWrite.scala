package chapter09

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite

class OrcFileWrite extends AnyFunSuite {
    test ("orc_write") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq(
            (1, 2, 3)
        ).toDF("a", "b", "c")

        df.write
          .format("orc")
          .mode(SaveMode.Overwrite)
          .save("/tmp/my-parquet-file.orc")
    }
}
