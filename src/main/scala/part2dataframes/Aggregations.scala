package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val dataPath = "src/main/resources/data/"
  val spark = SparkSession.builder()
    .appName("Aggregations and grouping")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json(s"$dataPath/movies.json")

  moviesDf.printSchema()

  // counting -- these are equivalent and count rows where genre is not null
  val genresCountDf = moviesDf.select(count(col("Major_Genre")))
  val genresCountDfToo = moviesDf.selectExpr("count(Major_Genre)")
  genresCountDf.show()
  genresCountDfToo.show()

  // select count(*)
  moviesDf.select(count("*").as("count_all")).show()

  // count distinct
  moviesDf.select(countDistinct("Major_Genre").as("num_genres")).show()

  // approximate count
  moviesDf.select(approx_count_distinct("Major_Genre")).show()

  // min and max
  val minRating = moviesDf
    .select(min("IMDB_Rating")
      .as("minimum_rating"))

  val maxRating = moviesDf
    .select(max("IMDB_Rating")
      .as("maximum_rating"))

  minRating.show()
  maxRating.show()

  // sum -- these expressions are equivalent
  moviesDf
    .select(sum("US_Gross")).show()
  moviesDf.selectExpr("sum(US_Gross)").show()

  // average
  val avgRating = moviesDf
    .select(avg("Rotten_Tomatoes_Rating")
      .as("avergate_rotten_tomatoes_rating"))
  val avgRatingExpr = moviesDf
    .selectExpr("avg(Rotten_Tomatoes_Rating) as avergate_rotten_tomatoes_rating")

  avgRating.show()
  avgRatingExpr.show()

  // data scients
  moviesDf
    .select(
      mean("Rotten_Tomatoes_Rating"),
      stddev("Rotten_Tomatoes_Rating")
    )
    .show()

  // Grouping
  // movies per genre
  val countByGenre = moviesDf
    .groupBy("Major_Genre")
    .count() // select count(*) from moviesDf group by major_genre
    .sort("count")
  countByGenre.show()

  // avg imdb rating by genre
  val avgRatingByGenre = moviesDf
    .groupBy("Major_Genre")
    .avg("IMDB_Rating").as("avg_imdb_rating")
  avgRatingByGenre.show()

  // using agg
  val aggregationsByGenre = moviesDf
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("num_movies"),
      avg("IMDB_Rating").as("avg_rating")
    )
    .orderBy("Avg_Rating")
  aggregationsByGenre.show()
}
