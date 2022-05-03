package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object AggregationsExercises extends App {

  val spark = SparkSession.builder()
    .appName("Aggregations Exercies")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val path = "src/main/resources/data/movies.json"

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json(path)

  /**
    * Exercises
    * 1. Sum all profits of all movies
    * 2. Count distinct directors
    * 3. Show mean and stddev of US gross revenue
    * 4. Compute average IMDB rating and average US gross revenue per director
    */

  // 1. Sum all profits of all movies
  moviesDf.select((
    col("US_Gross") +
      col("Worldwide_Gross") +
      col("US_DVD_Sales"))
    .as("total_gross"))
    .select(sum("total_gross"))
    .show()

  // 2. Count distinct directors
  moviesDf.select(
    countDistinct(
      col("Director"))
      .as("count_directors"))
    .show()

  // 3. Show mean and stddev of US gross revenue
  val revCol = "US_Gross"
  moviesDf.select(
    mean(revCol).as("mean_us_gross"),
    stddev(revCol).as("std_dev_us_gross")
  )
    .show()

  // 4. Compute average IMDB rating and average US gross revenue per director
  moviesDf.groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("avg_imdb_rating"),
      avg("US_Gross").as("avg_us_gross"),
      )
    .orderBy(col("avg_us_gross").desc)
    .show()
}
