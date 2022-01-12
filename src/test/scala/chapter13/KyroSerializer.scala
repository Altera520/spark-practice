package chapter13

import common.{CommonSpark, CommonUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite

class KyroSerializer extends AnyFunSuite{
    test ("basic_serialization") {
        class SomeClass extends Serializable {
            var someValue = 0
            def setSomeValue(i: Int) = {
                someValue = i
                this
            }
        }

        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        spark.sparkContext.parallelize(1 to 10).map(num => new SomeClass().setSomeValue(num))
    }

    test ("kyro_serialization") {
        case class MyClass1(id: Int, value: Int)

        val conf = new SparkConf().setMaster("master").setAppName("kyro")

        // kyro에 사용자 정의 클래스 사전에 직접 등록
        conf.registerKryoClasses(Array(classOf[MyClass1]))

        // SparkConf를 바탕으로 SparkContext 생성
        val sc = new SparkContext(conf)
    }
}
