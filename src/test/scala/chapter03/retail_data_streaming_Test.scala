package chapter03

import common.CommonSpark
import org.scalatest.funsuite.AnyFunSuite

class retail_data_streaming_Test extends AnyFunSuite {
    test("retail_data_streaming_test") {
        val spark = CommonSpark.createLocalSparkSession(retail_data.appName)
        val read_df = retail_data_streaming.read(spark)
        read_df.show(5)

        val select_df = retail_data.processing(read_df)
        select_df.show(5)
    }
}
