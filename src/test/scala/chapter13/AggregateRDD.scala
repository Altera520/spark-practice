package chapter13

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class AggregateRDD extends AnyFunSuite {
    test("countByKey") {
        val timeout = 1000L     // ms
        val confidence = 0.95

        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val pairRDD = Prepare.generateDataset(spark)

        pairRDD.countByKey()
        pairRDD.countByKeyApprox(timeout, confidence)
    }

    test("groupByKey") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val pairRDD = Prepare.generateDataset(spark)

        // groupByKey로 만들어진 그룹에 map 연산을 수행
        // 모든 익스큐터에서 함수를 적용하기전에 해당 키와 관련된 모든 값을 메모리로 읽어들여야
        // 특정 키에 데이터가 치우쳐져 있으면, skew때문에 OOM 발생가능
        pairRDD.groupByKey().map(row => (row._1, row._2.reduce(Prepare.addFunc))).collect
    }

    test("reduceByKey") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        
        Prepare.generateDataset(spark)
          // 각 파티션에서 reduce 작업을 수행,
          // 모든 값을 메모리에 유지 X
          // 최종 리듀스를 제외한 모든 작업은 개별 워커에서 처리, 연산 중에서 셔플 발생 X
          .reduceByKey(Prepare.addFunc)
          .collect()
          .foreach { item =>
              println(item)
          }
        /*
        (o,2), (r,2), (l,3), (d,1), (s,1), (p,1), (a,1), (w,1), (e,1), (k,1), (h,1)
         */
    }

    test("aggregate") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val nums = Prepare.nums(spark)
        // 인자로 전달된 첫번째 함수는 파티션 내에서, 두번째 함수는 모든 파티션에 걸쳐 수행
        println(nums.aggregate(0)(Prepare.maxFunc, Prepare.addFunc))    // 90
    }

    test("tree_aggregate") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val nums = Prepare.nums(spark)
        // 인자로 전달된 첫번째 함수는 파티션 내에서, 두번째 함수는 모든 파티션에 걸쳐 수행
        println(nums.treeAggregate(0)(Prepare.maxFunc, Prepare.addFunc, depth = 5))    // 90
    }

    test("aggregateByKey") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        Prepare.generateDataset(spark)
          // aggregate와 동일하나, 파티션 대신 키를 기준으로 연산을 수행
          .aggregateByKey(0)(Prepare.addFunc, Prepare.maxFunc)
          .collect()
    }

    test("combineByKey") {
        val valToCombiner = (value: Int) => List(value)
        val mergeValuesFunc = (vals: List[Int], valToAppend: Int) => valToAppend :: vals
        val mergeCombinerFunc = (vals1: List[Int], vals2: List[Int]) => vals1 ::: vals2

        val outputPartitions = 6    // 함수형 변수

        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        Prepare.generateDataset(spark)
          // 집계함수 대신 컴바이너를 사용, 컴바이너는 키를 기준으로 연산,
          // 이후 여러 컴바이너의 결과값을 병합하여 결과 반환
          .combineByKey(
              valToCombiner,        // 컴바이너
              mergeValuesFunc,      // 컴바이너
              mergeCombinerFunc,    // 컴바이너
              outputPartitions      // 사용자 정의 파티셔너, 출력 파티션수 지정
          )
          .collect().toSeq foreach { item =>
            println(item)
        }
    }

    test ("foldByKey") {
        // 결합 함수와 항등원인 '제로값'을 이용해 각 키의 값을 병합
        // 제로값은 결과에 여러번 사용될수 있으나, 결과를 변경할 수 X
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        Prepare.generateDataset(spark)
          .foldByKey(0)(Prepare.addFunc)
          .collect() foreach { item =>
            println(item)
        }
        /*
        (o,2), (r,2), (l,3), (d,1), (s,1), (p,1), (a,1), (w,1), (e,1), (k,1), (h,1)
         */
    }

    test ("cogroup") {
        // 스칼라는 최대 3개, 파이썬은 최대 2개의 KeyValue RDD를 그룹화 가능
        // 각 키를 기준으로 값을 결합, RDD에 대한 그룹 기반의 조인을 수행
        // 출력 파티션 수나, 클러스터에 데이터 분산 방식을 정확히 제어하기 위해 사용자 정의 파티션 함수를 파라미터로 사용
        import scala.util.Random

        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = spark.sparkContext.parallelize("Hello world spark".split(StringUtils.SPACE), 3)
        val distinctChars = words.flatMap(_.toLowerCase.toSeq).distinct

        val charRDD = distinctChars.map(c => (c, new Random().nextDouble()))
        val charRDD2 = distinctChars.map(c => (c, new Random().nextDouble()))
        val charRDD3 = distinctChars.map(c => (c, new Random().nextDouble()))

        // 그룹화된 키를 키로, 키와 관련된 모든 값을 값으로 하는 키-값 형태의 배열을 결과로 반환
        charRDD.cogroup(charRDD2, charRDD3)
    }
}
