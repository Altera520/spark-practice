package chapter21

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

class KafkaSourceAndSync extends AnyFunSuite{

    test("kafka_load_and write") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        /**
         * kafka source load
         */
        val ds = spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "my-kafka:9092")
          .option("subscribe", "spark_practice")
          // .option("subscribe", "spark_practice_1", "spark_practice_2") // 여러개의 토픽 수신
          // .option("subscribePattern", "spark_practice_*") // 토픽 패턴에 맞게 수신
          .load()

        /**
         * kafka sync write
         */
        val stream = ds.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          .writeStream
          .format("kafka")
          .option("checkpointLocation", "/Users/angyojun/workspace/kafka_bin/spark_checkpoint_tmp")
          .option("kafka.bootstrap.servers", "my-kafka:9092")
          .option("topic", "spark_practice_echo") // topic을 option에 지정할수도, selectExpr에 지정할수도
          .start()

        stream.awaitTermination()
    }
}
