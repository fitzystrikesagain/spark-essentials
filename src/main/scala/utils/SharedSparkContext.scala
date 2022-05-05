package utils

import org.apache.spark
import org.apache.spark.sql.SparkSession

object SharedSparkContext extends App {
  val spark = SparkSession
  .builder()
    .appName("Shared Spark Context")
    .config("spark.master", "local")
    .getOrCreate()
}
