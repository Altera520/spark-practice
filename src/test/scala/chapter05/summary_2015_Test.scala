package chapter05

import common.{CommonSpark, CommonUtil}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.dsl.expressions.{DslExpression, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.{col, desc, expr, lit}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class summary_2015_Test extends AnyFunSuite {
    test("summary_2015") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        val customSchema = StructType(
            Array(
                StructField("DEST_COUNTRY_NAME", StringType, true),
                StructField("ORIGIN_COUNTRY_NAME", StringType, true),
                StructField("count", LongType, false,
                    Metadata.fromJson("""{"hello": "world"}""")
                )
            )
        )
        spark.read.format("json").schema(customSchema)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")
          .show()

        spark.read.format("json").schema(customSchema)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")
          .printSchema()
    }

    test("col_test") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))

        // col("some_col")
        // column("some_col2")

        spark.read
          .format("json")
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")
          .printSchema()
    }

    test("df_col_ref") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        print {
            spark.read
              .format("json")
              .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")
              .first()
        }
    }

    test("mk_row") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val row = Row("Hello", null, 1, false)
        print(row.getInt(2))
        print(row.getInt(2).getClass)
        print(row(2))
        print(row(2).getClass)
    }

    test("df_test") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.createOrReplaceTempView("dfTable")

        val schema = StructType(
            Array(
                StructField("some", StringType, true),
                StructField("col", StringType, true),
                StructField("names", LongType, false)
            )
        )

        val row = Seq(Row("Hello", null, 1L))
        val rdd = spark.sparkContext.parallelize(row)
        val rdf = spark.createDataFrame(rdd, schema)
        rdf.show()

        // Scala 버전의 스파크 콘솔을 사용하는 경우 Seq 데이터 타입에 toDF 함수를 사용가능 (암시적 변환)
        // val myDf = Seq(("Hello", 2, 1L)).toDF("col1", "col2", "col3")
    }

    test("df_select_test") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.selectExpr(
            "avg(count)",
            "count(distinct(DEST_COUNTRY_NAME))"
        ).show(2)
    }

    test("literal_test") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.select(expr("*"), lit(1).as("One")).show(2)
        df.select(expr("*"), lit(1).alias("One")).show(2)
    }

    test("withcolumn_test1") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        val df2 = df.withColumn("withinCountry", expr("dest_country_name == origin_country_name"))
          .toDF()

        df2.withColumnRenamed("withinCountry", "isMatch")
          .show(2)
    }

    test("withcolumn_test2") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.withColumn("ori", expr("ORIGIN_COUNTRY_NAME")).show()
    }

    test("col_drop") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(2)
    }

    test("cast") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.withColumn("count2", col("count").cast("string")).show()
    }

    test("where") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.where("count < 2").show(2)
        df.where(col("count") === col("count")).show(2)
    }

    test("distinct") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME")
          .distinct()
          .show(2)
    }

    test("sample") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        val seed = 5
        val withReplacement = false
        val fraction = 0.5

        print(df.sample(withReplacement, fraction, seed).count())
    }

    test("random_sample") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        val seed = 5
        val dfs = df.randomSplit(Array(0.25, 0.75), seed)
        println(dfs(0).count())
        println(dfs(1).count())
    }

    test("union") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        val schema = df.schema

        val rows = Seq(
            Row("japan", "USA", 5L),
            Row("taiwan", "UK", 5L),
        )
        val rdd = spark.sparkContext.parallelize(rows)
        val newdf = spark.createDataFrame(rdd, schema)
        df.union(newdf)
          .where("count = 1")
          .where(col("ORIGIN_COUNTRY_NAME") =!= "UK")
          .show()
    }

    test("orderby") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/2015-summary.json")

        df.orderBy("count", "ORIGIN_COUNTRY_NAME").show(2)
        df.orderBy(col("count"), col("ORIGIN_COUNTRY_NAME")).show(2)

        df.orderBy(col("count"), col("ORIGIN_COUNTRY_NAME").desc).show(2)
        df.orderBy(col("count"), desc("ORIGIN_COUNTRY_NAME")).show(2)
    }

    test("sort_within_partitions") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/*-summary.json")
          .sortWithinPartitions("count")
    } 

    test("limit") {
        val spark = CommonSpark.createLocalSparkSession(CommonUtil.getAppName(this))
        val df = spark.read
          .format("json")
          .option("inferSchema", true)
          .load(s"${CommonUtil.P.rawDataPath}/data/flight-data/json/*-summary.json")
          .sortWithinPartitions("count")

        df.limit(5).show()
        df.orderBy(expr("count desc")).limit(5).show()
    }
}
