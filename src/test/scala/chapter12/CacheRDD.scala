package chapter12

import common.{CommonSpark, CommonUtil}
import org.apache.commons.lang3.StringUtils
import org.scalatest.funsuite.AnyFunSuite

class CacheRDD extends AnyFunSuite {
    test("cache") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        val words = "Hello World !".split(StringUtils.SPACE)
        val rdd = spark.sparkContext.parallelize(words)

        // 캐시와 저장은 메모리에 있는 데이터만을 대상으로 한다
        rdd.cache()
        // setName을 통해서 캐시된 RDD에 이름을 지정할 수 있다.
        rdd.setName("hello")

        // 저장소 수준은 org.apache.spark.storage.StorageLevel의 속성 중 하나로 지정가능
        rdd.getStorageLevel
    }
}
