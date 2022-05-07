package utils

import org.apache.spark.sql.SparkSession

object SharedSparkContext extends App {
  def createSession(logLevel: String = "OFF"): SparkSession = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Shared Spark Context")
      .config("spark.master", "local")
      .getOrCreate()
    spark.sparkContext.setLogLevel(logLevel)
    spark
  }

}
