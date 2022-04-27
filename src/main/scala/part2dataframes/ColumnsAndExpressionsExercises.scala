package part2dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions._

object ColumnsAndExpressionsExercises extends App {

  Logger.getRootLogger.setLevel(Level.WARN)
  val spark = SparkSession.builder()
    .appName("Columns and Expressions Exercise")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")
  val path = "src/main/resources/data/movies.json"

  /**
    * Exercises!
    *
    * 1. Read any two columns from the movies json
    *
    * 2. Create movies DF that sums gross profits: us, world, and dvd
    *
    * 3. Select comedy movies with IMDB rating >= 6.5
    */

    /* Schema
    root
     |-- Creative_Type: string (nullable = true)
     |-- Director: string (nullable = true)
     |-- Distributor: string (nullable = true)
     |-- IMDB_Rating: double (nullable = true)
     |-- IMDB_Votes: long (nullable = true)
     |-- MPAA_Rating: string (nullable = true)
     |-- Major_Genre: string (nullable = true)
     |-- Production_Budget: long (nullable = true)
     |-- Release_Date: string (nullable = true)
     |-- Rotten_Tomatoes_Rating: long (nullable = true)
     |-- Running_Time_min: long (nullable = true)
     |-- Source: string (nullable = true)
     |-- Title: string (nullable = true)
     |-- US_DVD_Sales: long (nullable = true)
     |-- US_Gross: long (nullable = true)
     |-- Worldwide_Gross: long (nullable = true)

     */

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json(path)
  moviesDf.printSchema()

  val df1 = moviesDf.select("Title", "Director")

  val df2 = moviesDf.groupBy("Title")
    .sum("US_DVD_Sales", "US_Gross",  "Worldwide_Gross")

  val df3 = moviesDf
    .select("Title", "Major_Genre", "IMDB_Rating")
    .filter("IMDB_Rating >= 6.5")

  df1.show
  df2.show
  df3.show
}