package part3typesdatasets

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object CommonTypes extends App {
  val spark: SparkSession = SparkSession.builder()
    .appName(name = "Common types")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  def buildJsonDf(filename: String): DataFrame =
    spark.read
      .option("inferSchema", "true")
      .json(path = s"src/main/resources/data/$filename.json")

  val moviesDf: DataFrame = buildJsonDf(filename = "movies")
  moviesDf.count()
  moviesDf.printSchema()
  moviesDf.show()

  // adding a plain value to a DF
  // lit works with booleans, strings, numbers, etc.
  moviesDf.select(cols =
    col(colName = "Title"),
    lit(literal = 47).as(alias = "plain_value")
  ).show()

  // booleans
  val dramaFilter: Column = col(colName = "Major_genre") equalTo "Drama"
  val goodRatingFilter: Column = col(colName = "IMDB_Rating") > 7.0
  val preferredFilter: Column = dramaFilter and goodRatingFilter

  moviesDf.select(col = "Title").where(condition = dramaFilter)
  // multiple ways of filtering
  val moviesWithGoodnessFlagsDf: DataFrame = moviesDf.select(
    cols = col(colName = "Title"),
    preferredFilter.as(alias = "good_movie")
  )
  moviesWithGoodnessFlagsDf
    .where(conditionExpr = "good_movie") // where(col("good_movie") === "true")
    .show()

  // negations
  moviesWithGoodnessFlagsDf
    .where(condition = not(col(colName = "good_movie")))
    .show()

  // numbers and math operators
  val moviesAvgRatingsDf: DataFrame = moviesDf.select(
    cols = col(colName = "Title"),
    ((col(colName = "Rotten_Tomatoes_Rating") / 10 + col(colName = "IMDB_Rating")) / 2).as(alias = "avg_rating")
  )
  moviesAvgRatingsDf.show()

  // correlation = number between -1 (negative correlation) and 1 (positive correlation)
  // corr is an action rather than transformation => 0.4259708986248317 is a weak positive correlation
  println(moviesDf.stat.corr(col1 = "Rotten_Tomatoes_Rating", col2 = "IMDB_Rating"))

  /**
    * Strings
    */
  // capitalization: initcap, lower, upper
  val carsDf: DataFrame = buildJsonDf(filename = "cars")
  carsDf.select(
    cols = initcap(col(colName = "Name"))
  ).show()

  carsDf.select(
    cols = lower(col(colName = "Name"))
  ).show()

  carsDf.select(
    cols = upper(col(colName = "Name"))
  ).show()

  // contains
  carsDf.select(col = "*")
    .where(condition = col(colName = "Name") contains "volkswagen")
    .show()

  // regex
  val regexString: String = "volkswagen|vw"
  val regexColName: String = "regex_extract"
  val vwDf: DataFrame = carsDf.select(
    cols = col(colName = "Name"),
    regexp_extract(col(colName = "Name"), exp = regexString, groupIdx = 0).as(alias = regexColName)
  ).where(condition = col(colName = regexColName) notEqual "")
    .drop(colName = regexColName)
  vwDf.show(truncate = false)

  vwDf.select(cols =
    col(colName = "Name"),
    regexp_replace(e = col(colName = "Name"), pattern = regexString, replacement = "People's Car").as(alias = regexColName)
  ).show(truncate = false)

  /**
    * Exercise
    *
    * 1. Filter carsDf by getCarNames
    */

  def getCarNames: List[String] = List(
    "Volkswagen",
    "Mercedes-Benz",
    "Ford",
  )

  val carRegexString: String = getCarNames.map(_.toLowerCase()).mkString("|")

  carsDf.select(cols =
    col(colName = "Name"),
    regexp_extract(col(colName = "Name"), exp = carRegexString, groupIdx = 0).as(alias = regexColName)
  )
    .where(condition = col(colName = regexColName) notEqual "")
    .drop(colName = regexColName)
    .show(truncate = false)

  // Version 2
  val carNameFilters: List[Column] = getCarNames.map(_.toLowerCase()).map(name => col(colName = "Name").contains(name))
  val bigFilter: Column = carNameFilters.fold(lit(literal = false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
  carsDf.filter(condition = bigFilter).show()
}
