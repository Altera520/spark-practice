package chapter13

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

import java.util.Random

class PartitionControl extends AnyFunSuite {
    test("coalesce") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello world spark".split(StringUtils.SPACE), 2)

        // 셔플링 없이 두 개의 파티션을 1개의 파티션으로 합침(파티션 수를 줄임), 네트워크 이동 발생 X
        words.coalesce(1).getNumPartitions
    }

    test("repartition") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello world spark".split(StringUtils.SPACE), 2)

        // 파티션 수를 늘리거나, 줄일 수 있지만, 처리 시 노드 간의 셔플이 발생, 네트워크 이동이 발생
        words.repartition(10)
    }

    test("user_define_partitioner_hashpartitioner") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(s"${CommonUtil.P.rawDataPath}/data/retail-data/all/")

        // 1. 구조적 API를 통해 RDD 획득
        val rdd = df.coalesce(10).rdd

        import org.apache.spark.HashPartitioner
        rdd.map(r => r(6)).take(5).foreach(println)
        val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)

        // 2. 파티셔너 적용
        // HashPartitioner나 RangePartitioner는 유용하지만 매우 기초적인 기능 제공, 이산형과 연속형 데이터를 다룰때 사용
        // 매우 큰 데이터나, 심각하게 치우친 키를 다뤄야 한다면 고급 파티셔닝 기능을 사용해야
        // skew를 해결하여 병렬성을 개선하고 OOM을 방지해야
        keyedRDD.partitionBy(new HashPartitioner(10)).take(10)
    }

    test("user_define_partitioner") {
        import org.apache.spark.Partitioner
        class DomainPartitioner extends Partitioner {
            override def numPartitions: Int = 3     // 파티션 수는 3개, 파티션 인덱스는 0 ~ 2

            override def getPartition(key: Any): Int = {
                val customerId = key match {
                    case k: Double => k.toInt
                }

                if (customerId == 17850.0 || customerId == 12583.0) 0 // 데이터량이 많은 17850.0과 12583.0은 0번째 파티션으로
                else new Random().nextInt(2) + 1    // 0 ~ 1 사이의 난수 생성 + 1 (1 또는 2 번째 파티션으로)
            }
        }

        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(s"${CommonUtil.P.rawDataPath}/data/retail-data/all/")

        df.show()

        // 1. 구조적 API를 통해 RDD 획득
        val rdd = df.coalesce(10).rdd
        rdd.map(r => r(6)).take(5).foreach(println)
        val keyedRDD = rdd.keyBy(row => row(6).asInstanceOf[Int].toDouble)

        // 2. 사용자 파티셔너 적용
        keyedRDD
          .partitionBy(new DomainPartitioner).map(_._1).glom().map(_.toSet.toSeq.length)
          .take(5)
    }
}
