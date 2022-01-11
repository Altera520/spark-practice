package chapter12

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class CheckpointingRDD extends AnyFunSuite{
    // DF API에서는 체크포인팅을 사용할 수 없다.
    // 체크포인팅은 RDD를 디스크에 저장하는 방식
    // 나중에 저장된 RDD를 참조할때는 원본 데이터소스를 다시 계산해 RDD를 생성하지 않고 디스크에 저장된 중간결과 파티션을 참조
    // 캐싱과 유사, 반복적인 연산 수행시 매우 유용
    test("checkpointing") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val words = "Hello World !".split(StringUtils.SPACE)
        val rdd = spark.sparkContext.parallelize(words)

        // 체크포인트 지정
        spark.sparkContext.setCheckpointDir("/tmp/path/for/checkpointing")

        // 체크포인팅
        rdd.checkpoint()
    }
}
