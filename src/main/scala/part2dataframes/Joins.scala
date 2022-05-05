package part2dataframes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Joins extends App {

  val spark = SparkSession.builder()
    // bands, guitarPlayers, and guitars
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  val path = "src/main/resources/data"
  val guitarsDf = spark
    .read
    .json(s"$path/guitars.json")

  val guitaristsDf = spark.read
    .option("inferSchema", "true")
    .json(s"$path/guitarPlayers.json")

  val bandsDf = spark
    .read
    .json(s"$path/bands.json")

  // inner joins
  val joinCondition = guitaristsDf.col("band") === bandsDf.col("id")
  val guitaristsBandsDf = guitaristsDf
    .join(bandsDf, joinCondition, "inner")
  val guitaristsBandsDfInner = guitaristsBandsDf

  // left outer -> everything in the inner join and all rows from left table
  // with nulls for missing data
  val guitaristsBandsDfLeftOuter = guitaristsDf
    .join(bandsDf, joinCondition, "left_outer")

  // right outer -> everything in the inner join and all rows from right table
  // with nulls for missing data
  val guitaristsBandsDfRightOuter = guitaristsDf
    .join(bandsDf, joinCondition, "right_outer")

  // full outer -> everything in the inner join and all rows from both tables
  val guitaristsBandsDfFullOuter = guitaristsDf
    .join(bandsDf, joinCondition, "outer")

  // guitaristsBandsDfInner.show()
  // guitaristsBandsDfLeftOuter.show()
  // guitaristsBandsDfRightOuter.show()
  // guitaristsBandsDfFullOuter.show()

  // semi-joins perform a join but exclude the right table
  /**
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   0|    [0]|  0|  Jimmy Page|
    * |   1|    [1]|  1| Angus Young|
    * |   3|    [3]|  3|Kirk Hammett|
    * +----+-------+---+------------+
    */
  guitaristsDf.join(bandsDf, joinCondition, "left_semi").show()

  // left anti-join -- only keep rows in left df where no rows in right df match
  /**
    * +----+-------+---+------------+
    * |band|guitars| id|        name|
    * +----+-------+---+------------+
    * |   2| [1, 5]|  2|Eric Clapton|
    * +----+-------+---+------------+
    */
  guitaristsDf
    .join(
      bandsDf,
      joinCondition,
      "left_anti")
    .show()

  // // things to bear in mind
  // // spark will not know which id column we're referring to, since both
  // // input datasets have an id column

  // guitaristsBandsDf.select(
  //   "id",
  //   "band"
  // )
  //   .show()

  // Option 1 - rename the column on which we are joining
  guitaristsDf
    .join(
      bandsDf.withColumnRenamed("id", "band"),
      "band")
    .show()

  // Option 2 - drop dup column
  guitaristsBandsDf.drop(bandsDf.col("id")).show()

  // Option 3 - rename the column and keep the data
  val bandsModDf = bandsDf.withColumnRenamed("id", "bandId")
  guitaristsDf
    .join(
      bandsModDf,
      joinCondition,
      "inner")
    .show()

  // Complex types
  //  guitaristsDf
  //    .join(
  //      guitarsDf.withColumnRenamed(
  //        "id",
  //        "guitarId"
  //      ),
  //      expr("array_contains(guitars, guitarId)")
  //    )
  //    .show()
}
