package common

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

case class AppConfig(config: Config) extends LazyLogging {
    /**
     * application 영역
     */
    lazy val rawDataPath = config.getString("app.rawDataPath")

    /**
     * spark 영역
     */
    lazy val sparkOptions = {
        val options = config.getConfig("spark.options")
        options.entrySet().map {
            entry => (entry.getKey, options.getString(entry.getKey))
        }.toMap
    }

    override def toString: String =
        s"""
           | rawDataPath = $rawDataPath
           | """.stripMargin

    logger.info(this.toString)
}
