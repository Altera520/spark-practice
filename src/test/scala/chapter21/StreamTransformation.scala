package chapter21

import chapter21.StreamTransformation.getStreaming
import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.functions.expr
import org.scalatest.funsuite.AnyFunSuite

object StreamTransformation {
    def getStreaming(appName: String) = {
        val spark = CommonSpark.createLocalSparkSession(appName)
        spark.conf.set("spark.sql.shuffle.partitions", 5)

        val static = spark.read.json(s"${CommonUtil.P.rawDataPath}/data/activity-data/")
        val dataSchema = static.schema

        val streaming = spark.readStream.schema(dataSchema)
          .option("maxFilesPerTrigger", 1)
          .json(s"${CommonUtil.P.rawDataPath}/data/activity-data/")

        (streaming, spark, static)
    }
}

class StreamTransformation extends AnyFunSuite {
    test("select_and_filter") {
        val (streaming, _, _) = getStreaming(CommonUtil.getAppName(this))

        val simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))
          .where("stairs")
          .where("gt is not null")
          .select("gt", "model", "arrival_time", "creation_time")
          .writeStream
          .queryName("simple_transform")
          .format("memory")
          .outputMode("append")
          .start()

        simpleTransform.awaitTermination()
    }

    test("aggregation") {
        // read
        val (streaming, spark, _) = getStreaming(CommonUtil.getAppName(this))

        // processing
        val df = streaming.cube("gt", "model").avg()
          .drop("avg(Arrival_time)")
          .drop("avg(Creation_time)")
          .drop("avg(Index)")

        // load
        val out = df.writeStream.queryName("device_counts")
          .format("memory")
          .outputMode("complete")
          .start()

        out.awaitTermination()

        (1 to 5).foreach { x =>
            spark.sql("select * from device_counts").show
            Thread.sleep(1000)
        }
    }

    test("join") {
        val (streaming, _, static) = getStreaming(CommonUtil.getAppName(this))

        val historicalAgg = static.groupBy("gt", "model").avg()

        val df = streaming.drop("Arrival_Time", "Creation_Time", "Index")
          .cube("gt", "model").avg()
          .join(historicalAgg, Seq("gt", "model"))
        // 2.2v에서는 오른쪽 스트림 데이터 대상으로 full outer, left outer join만 지원

        val out = df.writeStream
          .queryName("device_counts")
          .format("memory")
          .outputMode("complete")
          .start()

        out.awaitTermination()
    }
}
