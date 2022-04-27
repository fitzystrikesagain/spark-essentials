package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressionsExercises extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")
  val path = "src/main/resources/data"

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json(s"$path/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting)
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // Fancier interpolated string, returns a column object
    expr("Origin"), // EXPRESSION
  )

  // Select with plain column names. Can't' be mixed with the above methods
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDf = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("weight_in_kg_expr")
  )
  carsWithWeightsDf.show()

  // selectExpr
  val carsWithSelectExprWeightsDf = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing
  val carsWithKg3Df = carsDF.withColumn("Weight_in_kg_3", expr("Weight_in_lbs / 2.2"))

  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  carsWithColumnRenamed.show()
  // column names with spaces or hyphens need backticks
  val carsWithColumnRenamed2 = carsDF.withColumnRenamed("Weight_in_lbs", "`Weight in pounds`")
  carsWithColumnRenamed2.show()


  // dropping/removing columns
  carsWithColumnRenamed.drop("Cylinders", "Displacement").show()

  // filtering -- used often in data engineering
  println("\n\n\n**************Filtering**************")
  carsDF.filter(col("Origin") =!= "USA").show()
  carsDF.where(col("Origin") =!= "USA").show()

  // filtering with expression strings
  carsDF.filter("Origin = 'USA'").show()

  // chaining filters
  val americanPowerfulCars = carsDF
    .filter(col("Origin") === "USA")
    .filter(col("Horsepower") > 150)

  // passing multiple filters
  val americanPowerfulCars2 = carsDF
    .filter(
      (col("Origin") === "USA") and
        (col("Horsepower") > 150)
    )
  val americanPowerfulCars3 = carsDF
    .filter("Origin = 'USA'")


  americanPowerfulCars.show()
  americanPowerfulCars2.show()
  americanPowerfulCars3.show()

  // unioning -- adding more rows
  val moreCarsDf = spark.read
    .option("inferSchema", "true")
    .json(s"$path/more_cars.json")

  // only works if the two DFs have the same schema
  val allCarsDf = carsDF.union(moreCarsDf)

  // distinct values
  val allCountriesDf = carsDF
    .select("Origin")
    .distinct()
  allCountriesDf.show()
}