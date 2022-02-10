package chapter21

import common.{CommonSpark, CommonUtil}
import org.scalatest.funsuite.AnyFunSuite

class BasicStream extends AnyFunSuite{
    test("read_activity_data") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val static = spark.read.json(s"${CommonUtil.P.rawDataPath}/data/activity-data/")
        val dataSchema = static.schema

        static.printSchema()
        /*
        root
         |-- Arrival_Time: long (nullable = true)
         |-- Creation_Time: long (nullable = true)
         |-- Device: string (nullable = true)
         |-- Index: long (nullable = true)
         |-- Model: string (nullable = true)
         |-- User: string (nullable = true)
         |-- gt: string (nullable = true)
         |-- x: double (nullable = true)
         |-- y: double (nullable = true)
         |-- z: double (nullable = true)
         */

        // 셔플 파티션을 많이 생성하지 않도록 제한한다.
        spark.conf.set("spark.sql.shuffle.partitions", 5)

         /*inferSchema를 사용하고 싶으면 명시적으로 설정해줘야한다.
         모르는 사이 데이터가 바뀔수도 있으니 운영환경에서는 스키마 추론 방식을 사용하면 안된다
         다음라인에서는 inferSchema를 적용하지 않는다.*/
        val streaming = spark.readStream.schema(dataSchema)
          /* 폴더 내 전체 파일을 얼마나 빨리 읽을지 제어
           값을 낮게 잡으면 트리거당 하나의 파일을 읽게 만들어 스트림의 흐름을 인위적으로 제한할 수
           구조적 증분 스트리밍 처리를 보기 위해서 1로 설정, 운영 환경에서는 동일한 설정을 사용하면 안된다*/
          .option("maxFilesPerTrigger", 1)
          .json(s"${CommonUtil.P.rawDataPath}/data/activity-data/")

        // 스트리밍 DF는 지연처리로 동작한다.
        // 스트림 처리를 시작하는 액션 호출 전 스트리밍 DF에 대한 트랜스포메이션을 지정할수 있다.
        val activityCounts = streaming.groupBy("gt").count()

        val activityQuery = activityCounts.writeStream
          // 스트림 처리에 사용하는 쿼리명을 지정
          .queryName("activity_counts")
          // 스트림 결과를 메모리 싱크에 저장한다.
          .format("memory")
          // 출력 모드 설정
          .outputMode("complete")
          // 액션 호출
          .start()

        // 쿼리 종료시까지 대기(쿼리 실행중 드라이버 프로세스가 종료되는 상황을 방지)
        activityQuery.awaitTermination()

        // 실행중인 스트림 목록을 확인가능
        println(spark.streams.active)

        (1 to 5).foreach { x =>
            spark.sql("SELECT * FROM activity_counts").show()
            Thread.sleep(1000)
        }
    }
    /*
    22/02/10 23:52:04 INFO MicroBatchExecution: Streaming query made progress: {
      "id" : "5406aa67-d360-46ea-9fe5-b32bc5532dab",
      "runId" : "1de75a13-3ffd-4fc9-95e5-211a8dc1fac0",
      "name" : "activity_counts",
      "timestamp" : "2022-02-10T14:52:00.286Z",
      "batchId" : 0,
      "numInputRows" : 78012,
      "inputRowsPerSecond" : 0.0,
      "processedRowsPerSecond" : 18503.795066413662,
      "durationMs" : {
        "addBatch" : 3491,
        "getBatch" : 71,
        "latestOffset" : 193,
        "queryPlanning" : 202,
        "triggerExecution" : 4211,
        "walCommit" : 132
      },
      "stateOperators" : [ {
        "numRowsTotal" : 7,
        "numRowsUpdated" : 7,
        "memoryUsedBytes" : 2736,
        "numRowsDroppedByWatermark" : 0,
        "customMetrics" : {
          "loadedMapCacheHitCount" : 0,
          "loadedMapCacheMissCount" : 0,
          "stateOnCurrentVersionSizeBytes" : 2016
        }
      } ],
      "sources" : [ {
        "description" : "FileStreamSource[file:/Users/angyojun/workspace/SparkPractice/Spark-The-Definitive-Guide/data/activity-data]",
        "startOffset" : null,
        "endOffset" : {
          "logOffset" : 0
        },
        "numInputRows" : 78012,
        "inputRowsPerSecond" : 0.0,
        "processedRowsPerSecond" : 18503.795066413662
      } ],
      "sink" : {
        "description" : "MemorySink",
        "numOutputRows" : 7
      }
    }
     */
}
