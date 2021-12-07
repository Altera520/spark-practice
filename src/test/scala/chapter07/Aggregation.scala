package chapter07

import common.CommonSpark
import common.CommonUtil.P
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{approx_count_distinct, avg, collect_list, collect_set, corr, count, countDistinct, covar_pop, covar_samp, expr, first, kurtosis, last, min, skewness, stddev_pop, stddev_samp, sum, sumDistinct, var_pop, var_samp}
import org.scalatest.funsuite.AnyFunSuite

class Aggregation extends AnyFunSuite{
    test("count action") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/all/*.csv")
          .coalesce(5)          // 파티션을 훨씬 적은수로 분할 -> 적은 양의 데이터를 가진 수많은 파일이 존재하므로

        df.cache()  // 빠르게 접근할수 있도록 캐싱
        df.count()  // count메서드는 액션, count 메서드는 데이터셋의 전체 크기를 알아보는 용도로 사용하지만 DF 캐싱 작업을 수행하는 용도로 사용되기도
    }

    test("count transformation") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(count("StockCode")).show()  // 여기서 count는 트랜스포메이션
    }

    test("count distinct") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(countDistinct("StockCode")).show()  // 여기서 count는 트랜스포메이션
    }

    test("approx count distinct") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(approx_count_distinct("StockCode", 0.1)).show() // rsd는 최대 추정 오류율
        df.select(approx_count_distinct("StockCode", 0.05)).show() // rsd는 최대 추정 오류율
    }

    test("first last") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(first("StockCode"), last("StockCode")).show()
    }

    test("min max") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(min("Quantity"), functions.max("Quantity")).show()
    }

    test("sum sum_distinct") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(sum("Quantity"), sumDistinct("Quantity")).show()
    }

    test("avg") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(
            count("Quantity") as "total_transactions",
            sum("Quantity") as "total_purchases",
            avg("Quantity") as "avg_purchases",
            expr("mean(Quantity)") as "mean_purchases",
        ).selectExpr(
            "total_purchases/total_transactions",
            "avg_purchases",
            "mean_purchases"
        ).show(false)
        /*
        +--------------------------------------+----------------+----------------+
        |(total_purchases / total_transactions)|avg_purchases   |mean_purchases  |
        +--------------------------------------+----------------+----------------+
        |9.55224954743324                      |9.55224954743324|9.55224954743324|
        +--------------------------------------+----------------+----------------+
         */
    }

    test("sample population") {
        // 표본표준편차, 모표준편차
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(
            var_pop("Quantity"),
            var_samp("Quantity"),
            stddev_pop("Quantity"),
            stddev_samp("Quantity"),
        ).show(false)
        /*
        +------------------+------------------+--------------------+---------------------+
        |var_pop(Quantity) |var_samp(Quantity)|stddev_pop(Quantity)|stddev_samp(Quantity)|
        +------------------+------------------+--------------------+---------------------+
        |47559.303646608765|47559.391409298456|218.0809566344773   |218.0811578502335    |
        +------------------+------------------+--------------------+---------------------+
         */
    }

    test("skewness kurtosis") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(
            skewness("Quantity"),
            kurtosis("Quantity")
        ).show()
        /*
        +-------------------+------------------+
        | skewness(Quantity)|kurtosis(Quantity)|
        +-------------------+------------------+
        |-0.2640755761052901|119768.05495539501|
        +-------------------+------------------+
         */
    }

    test("cov corr") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.select(
            corr("InvoiceNo", "Quantity"),
            covar_samp("InvoiceNo", "Quantity"),
            covar_pop("InvoiceNo", "Quantity"),
        ).show()
        /*
        +-------------------------+-------------------------------+------------------------------+
        |corr(InvoiceNo, Quantity)|covar_samp(InvoiceNo, Quantity)|covar_pop(InvoiceNo, Quantity)|
        +-------------------------+-------------------------------+------------------------------+
        |     4.912186085616477E-4|             1052.7280543862437|            1052.7260778701393|
        +-------------------------+-------------------------------+------------------------------+
         */
    }

    test("complex data type aggregation") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.agg(
            collect_set("Country"),
            collect_list("Country")
        ).show()
        /*
        +--------------------+---------------------+
        |collect_set(Country)|collect_list(Country)|
        +--------------------+---------------------+
        |[Portugal, Italy,...| [United Kingdom, ...|
        +--------------------+---------------------+
         */
    }
}
