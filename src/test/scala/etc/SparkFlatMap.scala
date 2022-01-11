package etc

import common.CommonSpark
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

abstract class Model

case class TicketModel(
    ticketId: Int,
    userId: String,
    startDate: Timestamp,
    endDate: Timestamp,
) extends Model

case class TicketNormalizationModel(
    ticketId: Int,
    userId: String,
    elapsed: Long
) extends Model

class SparkFlatMap extends AnyFunSuite {


    test("spark_flatmap_test") {
        val appName = this.getClass.getName.replace("$", "")
        val spark = CommonSpark.createLocalSparkSession(appName)
        import spark.implicits._

        val df = Seq(
            (1, "a", "2018-01-01", "2018-01-03"),
            (2, "b", "2018-01-01", "2018-01-03"),
            (3, "c", "2018-01-01", "2018-01-07"),
            (4, "a", "2018-01-04", "2018-01-06")
        ).toDF("ticket_id", "user_id", "start_date", "end_date")

        spark
          .read
          .format("parquet")
          .load("/Users/angyojun/Desktop/maasbibig/dataset/maasbi_big_data/dm_cs_op_anl_dd_sm")
          .orderBy($"p_crtn_dt".desc)
          .show()
    }
}
