package chapter07

import common.CommonSpark
import common.CommonUtil.P
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, count, countDistinct, dense_rank, expr, grouping_id, rank, row_number, sum, to_date, to_timestamp}
import org.scalatest.funsuite.AnyFunSuite

class Group extends AnyFunSuite{
    test("group by") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.groupBy("InvoiceNo", "CustomerId").count().show()
    }

    test("count") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.groupBy("InvoiceNo").agg(
            count("Quantity") as "quan",
            expr("count(Quantity)")
        ).show(false)
        /*
        +---------+----+---------------+
        |InvoiceNo|quan|count(Quantity)|
        +---------+----+---------------+
        |574966   |8   |8              |
        |575091   |38  |38             |
        |578057   |28  |28             |
        |537252   |1   |1              |
        |578459   |8   |8              |
        |C578132  |1   |1              |
        |578292   |72  |72             |
        |576112   |20  |20             |
        |577022   |38  |38             |
        |574592   |8   |8              |
        |C576393  |2   |2              |
        |577511   |46  |46             |
        |577541   |21  |21             |
        |580739   |2   |2              |
        |580906   |4   |4              |
        |573726   |1   |1              |
        |575671   |20  |20             |
        |570264   |1   |1              |
        |570281   |3   |3              |
        |569823   |69  |69             |
        +---------+----+---------------+
         */
    }

    test("map group by") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        df.groupBy("InvoiceNo").agg(
            "Quantity" -> "avg",
            "Quantity" -> "stddev_pop",
        ).show(5, false)
        /*
        +---------+------------------+--------------------+
        |InvoiceNo|avg(Quantity)     |stddev_pop(Quantity)|
        +---------+------------------+--------------------+
        |574966   |6.0               |3.640054944640259   |
        |575091   |11.552631578947368|5.008925551458656   |
        |578057   |4.607142857142857 |8.755974636597271   |
        |537252   |31.0              |0.0                 |
        |578459   |28.0              |26.0                |
        +---------+------------------+--------------------+
         */
    }

    test("window function") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/2010-12-01.csv")

        // to_date로 변환하면 HH:mm:ss가 짤린다.
        val dfWithDate = df.withColumn(
            "date",
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
        )
        /*
        +-------------------+
        |date               |
        +-------------------+
        |2010-12-01 08:26:00|
        |2010-12-01 08:26:00|
        +-------------------+
         */

        val windowSpec = Window
          .partitionBy("CustomerId", "date")
          .orderBy(col("Quantity").desc)                    // 파티션의 정렬방식
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)  // 첫 row부터 현재 row까지

        val maxPurchaseQuantity = functions.max(col("Quantity")).over(windowSpec)
        val purchaseDenseRank = dense_rank().over(windowSpec)       // 동일한 순위를 하나의 건수로 취급 ex. 1, 2, 2, 3
        val purchaseRank = rank().over(windowSpec)                  // 동일한 값에 대해 동일한 순서를 부여 ex. 1, 2, 2, 4
        val rowNumber = row_number().over(windowSpec)               // 동일한 값이라도 고유한 순서 부여 ex. 1, 2, 3, 4

        dfWithDate.where("CustomerId is not null")
          .orderBy("CustomerId")
          .select(
              col("CustomerId"),
              col("date"),
              col("Quantity"),
              purchaseRank as "quantityRank",
              purchaseDenseRank as "quantityDenseRank",
              rowNumber as "rowNumber",
              maxPurchaseQuantity as "maxPurchaseQuantity"
          ).show(false)
        /*
        +----------+-------------------+--------+------------+-----------------+---------+-------------------+
        |CustomerId|date               |Quantity|quantityRank|quantityDenseRank|rowNumber|maxPurchaseQuantity|
        +----------+-------------------+--------+------------+-----------------+---------+-------------------+
        |13448.0   |2010-12-01 10:52:00|24      |1           |1                |1        |24                 |
        |13448.0   |2010-12-01 10:52:00|12      |2           |2                |2        |24                 |
        |13448.0   |2010-12-01 10:52:00|12      |2           |2                |3        |24                 |
        |13448.0   |2010-12-01 10:52:00|12      |2           |2                |4        |24                 |
        |13448.0   |2010-12-01 10:52:00|12      |2           |2                |5        |24                 |
        |13448.0   |2010-12-01 10:52:00|12      |2           |2                |6        |24                 |
        |13448.0   |2010-12-01 10:52:00|9       |7           |3                |7        |24                 |
        |13448.0   |2010-12-01 10:52:00|9       |7           |3                |8        |24                 |
        |13448.0   |2010-12-01 10:52:00|8       |9           |4                |9        |24                 |
        |13448.0   |2010-12-01 10:52:00|8       |9           |4                |10       |24                 |
        |13448.0   |2010-12-01 10:52:00|6       |11          |5                |11       |24                 |
        |13448.0   |2010-12-01 10:52:00|6       |11          |5                |12       |24                 |
        |13448.0   |2010-12-01 10:52:00|6       |11          |5                |13       |24                 |
        |13448.0   |2010-12-01 10:52:00|4       |14          |6                |14       |24                 |
        |13448.0   |2010-12-01 10:52:00|4       |14          |6                |15       |24                 |
        |13448.0   |2010-12-01 10:52:00|4       |14          |6                |16       |24                 |
        |13448.0   |2010-12-01 10:52:00|2       |17          |7                |17       |24                 |
        |14606.0   |2010-12-01 16:58:00|4       |1           |1                |1        |4                  |
        |14606.0   |2010-12-01 16:58:00|2       |2           |2                |2        |4                  |
        |14606.0   |2010-12-01 16:58:00|2       |2           |2                |3        |4                  |
        +----------+-------------------+--------+------------+-----------------+---------+-------------------+
         */
    }

    test("group set") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        val dfWithDate = df.withColumn(
            "date",
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
        )

        val dfNoNull = dfWithDate.drop()
        dfNoNull.createOrReplaceTempView("dfNoNull")

        // group by 대상 컬럼은 df에 포함된다.
        dfNoNull.groupBy("CustomerId", "stockCode")
          .agg(
              sum(col("Quantity"))
          ).orderBy(col("CustomerId").desc, col("stockCode").desc)
          .show(5, false)
        /*
        +----------+---------+-------------+
        |CustomerId|stockCode|sum(Quantity)|
        +----------+---------+-------------+
        |18287.0   |85173    |48           |
        |18287.0   |85040A   |48           |
        |18287.0   |85039B   |120          |
        |18287.0   |85039A   |96           |
        |18287.0   |84920    |4            |
        +----------+---------+-------------+
         */

        // 그룹화 셋으로도 위와같이 동일한 작업을 처리할 수
        spark.sql(
            """
              |select CustomerId, stockCode, sum(Quantity)
              |  from dfNoNull
              | group by CustomerId, stockCode grouping sets((CustomerId, stockCode))
              | order by CustomerId desc, stockCode desc
              |""".stripMargin)
          .show(5, false)
        /*
        +----------+---------+-------------+
        |CustomerId|stockCode|sum(Quantity)|
        +----------+---------+-------------+
        |18287.0   |85173    |48           |
        |18287.0   |85040A   |48           |
        |18287.0   |85039B   |120          |
        |18287.0   |85039A   |96           |
        |18287.0   |84920    |4            |
        +----------+---------+-------------+
         */

        spark.sql(
            """
              |select CustomerId, stockCode, sum(Quantity)
              |  from dfNoNull
              | group by CustomerId, stockCode grouping sets((CustomerId, stockCode), ())
              | order by sum(Quantity) desc, CustomerId desc, stockCode desc
              |""".stripMargin)
          .show(5, false)
        /*
        +----------+---------+-------------+
        |CustomerId|stockCode|sum(Quantity)|
        +----------+---------+-------------+
        |null      |null     |5176450      |      // 모두 null인 row는 그룹화된 컬럼에서 집계된 전체 결과를 나타냄
        |13256.0   |84826    |12540        |
        |17949.0   |22197    |11692        |
        |16333.0   |84077    |10080        |
        |16422.0   |17003    |10077        |
        +----------+---------+-------------+
         */
    }

    test("rollup") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        val dfWithDate = df.withColumn(
            "date",
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
        )
        val dfNoNull = dfWithDate.drop()
        val rolledUpDF = dfNoNull.rollup("date", "country")
          .agg(sum("Quantity"))
          .selectExpr("date", "country", "`sum(Quantity)` as total_quantity")
          .orderBy("date")
        rolledUpDF.show(6, false)
        /*
        +-------------------+--------------+--------------+
        |date               |country       |total_quantity|
        +-------------------+--------------+--------------+
        |null               |null          |5176450       |
        |2010-12-01 08:26:00|null          |40            |
        |2010-12-01 08:26:00|United Kingdom|40            |
        |2010-12-01 08:28:00|United Kingdom|12            |
        |2010-12-01 08:28:00|null          |12            |
        |2010-12-01 08:34:00|United Kingdom|98            |
        +-------------------+--------------+--------------+
         */
    }

    test("cube") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        val dfWithDate = df.withColumn(
            "date",
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
        )
        val dfNoNull = dfWithDate.drop()
        val rolledUpDF = dfNoNull.cube("date", "country")
          .agg(sum("Quantity"))
          .selectExpr("date", "country", "`sum(Quantity)` as total_quantity")
          .orderBy("date")
        rolledUpDF.show(6, false)
        /*
        +----+--------------------+--------------+
        |date|country             |total_quantity|
        +----+--------------------+--------------+
        |null|United Arab Emirates|982           |
        |null|Iceland             |2458          |       // 모든 date, Iceland
        |null|Malta               |944           |
        |null|Poland              |3653          |
        |null|Finland             |10666         |
        |null|Lithuania           |652           |
        +----+--------------------+--------------+
         */
    }

    test("group metadata") {
        val spark = CommonSpark.createLocalSparkSession("test")
        val df = spark
          .read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(s"${P.rawDataPath}/data/retail-data/by-day/*.csv")

        val dfWithDate = df.withColumn(
            "date",
            to_timestamp(col("InvoiceDate"), "yyyy-MM-dd HH:mm:ss")
        )
        val dfNoNull = dfWithDate.drop()
        val cubeDF = dfNoNull.cube("date", "country", "customerid")
          .agg(
              grouping_id(),                    // 집계 수준
              sum("Quantity")
          )
          .orderBy(col("grouping_id()").desc)
        cubeDF.show(false)
        /*
        +----+-------+----------+-------------+-------------+
        |date|country|customerid|grouping_id()|sum(Quantity)|
        +----+-------+----------+-------------+-------------+
        |null|null   |null      |7            |5176450      |   //모든 date, 모든 country, 모든 customerid
        |null|null   |16153.0   |6            |1073         |   //모든 date, 모든 country, 16153 customerid
        |null|null   |13917.0   |6            |228          |
        |null|null   |15645.0   |6            |256          |
        |null|null   |16932.0   |6            |87           |
        |null|null   |12498.0   |6            |159          |
        |null|null   |15153.0   |6            |198          |
        |null|null   |17301.0   |6            |149          |
        |null|null   |17481.0   |6            |1060         |
        |null|null   |15596.0   |6            |548          |
        |null|null   |15105.0   |6            |1211         |
        |null|null   |13370.0   |6            |336          |
        |null|null   |14167.0   |6            |150          |
        |null|null   |16026.0   |6            |243          |
        |null|null   |14356.0   |6            |266          |
        |null|null   |13763.0   |6            |725          |
        |null|null   |14520.0   |6            |146          |
        |null|null   |18252.0   |6            |239          |
        |null|null   |14523.0   |6            |251          |
        |null|null   |15466.0   |6            |246          |
        +----+-------+----------+-------------+-------------+
         */

        val cubeDF2 = dfNoNull.cube("date", "country")
          .agg(
              grouping_id(),
              sum("Quantity")
          )
          .orderBy(col("grouping_id()").desc)
        cubeDF2.show(false)
        /*
        +----+--------------------+-------------+-------------+
        |date|country             |grouping_id()|sum(Quantity)|
        +----+--------------------+-------------+-------------+
        |null|null                |3            |5176450      |
        |null|RSA                 |2            |352          |
        |null|France              |2            |110480       |
        |null|Canada              |2            |2763         |
        |null|Singapore           |2            |5234         |
        |null|USA                 |2            |1034         |
        |null|United Kingdom      |2            |4263829      |
        |null|Lebanon             |2            |386          |
        |null|Poland              |2            |3653         |
        |null|United Arab Emirates|2            |982          |
        |null|Germany             |2            |117448       |
        |null|European Community  |2            |497          |
        |null|Italy               |2            |7999         |
        |null|Japan               |2            |25218        |
        |null|Czech Republic      |2            |592          |
        |null|Sweden              |2            |35637        |
        |null|Finland             |2            |10666        |
        |null|Lithuania           |2            |652          |
        |null|Iceland             |2            |2458         |
        |null|Malta               |2            |944          |
        +----+--------------------+-------------+-------------+
         */
    }

    test("pivot") {
        val spark = CommonSpark.createLocalSparkSession("test")
        import spark.implicits._

        val df = Seq(
            ("tester1", "2021-01-01", 0),
            ("tester1", "2021-01-02", 1),
            ("tester1", "2021-01-03", 1),
            ("tester1", "2021-01-04", 1),
            ("tester1", "2021-01-05", 0),
            ("tester2", "2021-01-04", 2),
            ("tester2", "2021-01-05", 7),
            ("tester2", "2021-01-06", 0),
        ).toDF("id", "date", "daygap")

        df.groupBy("id")
          .pivot("daygap")              // daygap row를 컬럼으로 변환
          .agg(countDistinct("date"))
          .show(false)
        /*
        +-------+---+----+----+----+
        |id     |0  |1   |2   |7   |
        +-------+---+----+----+----+
        |tester1|2  |3   |null|null|
        |tester2|1  |null|1   |1   |
        +-------+---+----+----+----+
         */
    }
}
