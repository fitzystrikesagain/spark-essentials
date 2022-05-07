package part3typesdatasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, StringType, StructField, StructType}
import utils.SharedSparkContext.createSession

object ComplexTypes extends App {
  val spark = createSession()
  val path = "src/main/resources/data"

  val moviesDf = spark.read
    .option("inferSchema", "true")
    .json(s"$path/movies.json")


  val moviesWithReleaseDates = moviesDf.select(
    col(colName = "Title"),
    // release date as a date type
    to_date(col(colName = "Release_Date"), fmt = "d-MMM-yy").as("Release_Date"))


  moviesWithReleaseDates
    .withColumn(colName = "today", col = current_date()) // today's date
    .withColumn(colName = "right_now", col = current_timestamp()) // this second
    // today - release date in years rounded to 2 decimals
    .withColumn(colName = "age", col = datediff(end = col(colName = "today"), start = col(colName = "Release_Date")) / 365)
  //    .show()

  moviesWithReleaseDates.select("*")
    .where(col("Release_Date").isNull)
  //    .show(truncate = false)

  /**
    * Exercises
    * 1. How do we deal with multiple formats?
    * 2. Read stocks data and correctly parse dates
    */
  // 1. Parse dataframe multiple times with multiple times then union DF. Not practical
  // for large datasets, so drop invalid date formats

  // 2. Read stocks data
  //  val stockSchema = StructType(Array(
  //    StructField("symbol", StringType),
  //    StructField("date", DateType),
  //    StructField("price", FloatType),
  //  ))

  val stocksDf = spark.read
    .options(Map(
      "inferSchema" -> "true",
      "header" -> "true",
      "dateFormat" -> "MMM d yyyy",
    ))
    .csv(s"$path/stocks.csv")

  stocksDf
    .withColumn("actual_date", to_date(col("date"), "MMM d yyyy"))
  //    .show()

  // Structures: groups of columns aggregated into one
  // 1. With col operators
  moviesDf.select(
    col("Title").as("title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("profit"))
    .select(
      col("title"),
      col("profit").getField("US_Gross").as("us_profit"))
  //    .show()

  // 2. with expression strings
  moviesDf.selectExpr("Title", "(US_Gross, Worldwide_Gross) as profit")
    .selectExpr("Title", "Profit.US_Gross")
  //    .show()

  // Arrays
  val moviesWithWords = moviesDf.select(
    col("Title"),
    split(col("Title"), " |,").as("title_words"))

  moviesWithWords.select(
    col("Title"),
    expr("title_words[0]").as("first_word_in_title"),
    size(col("title_words").as("number_title_words")),
    array_contains(col("title_words"), "Love").as("title_contains_love"))
    .show(truncate = false)
}
