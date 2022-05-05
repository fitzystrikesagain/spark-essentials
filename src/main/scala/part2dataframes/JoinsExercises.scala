package part2dataframes

import org.apache.spark.sql.{Column, DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

object JoinsExercises extends App {

  /**
    * Exercises
    * - show all employees and their max salaries -> salaries
    * - show all employees who were never managers -> dept_manager
    * - find job titles of the 10 best-paid employees -> titles
    */
  val spark: SparkSession = SparkSession.builder()
    .appName(name = "Joins exercises")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("OFF")

  def readTableFromJdbc(tableName: String): DataFrame = {
    val driver: String = "org.postgresql.Driver"
    val url: String = "jdbc:postgresql://localhost:5432/rtjvm"
    val user: String = "docker"
    val password: String = "docker"

    spark.read
      .format(source = "jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .load()
  }

  val salariesDf: DataFrame = readTableFromJdbc(tableName = "salaries")
  val deptManagerDf: DataFrame = readTableFromJdbc(tableName = "dept_manager")
  val titlesDf: DataFrame = readTableFromJdbc(tableName = "titles")
  val employeesDf: DataFrame = readTableFromJdbc(tableName = "employees")

  // show all employees and their max salaries
  println("show all employees and their max salaries")
  val maxSalariesPerEmployeeDf: Dataset[Row] = salariesDf
    .groupBy(col1 = "emp_no")
    .agg(expr = max(columnName = "salary").as(alias = "max_salary"))

  val employeeSalariesDf = employeesDf.join(
    right = maxSalariesPerEmployeeDf,
    usingColumn = "emp_no"
  )

  employeeSalariesDf.show()


  // show all employees who were never manager
  println("show all employees who were never manager")
  val managerJoinCondition: Column =
    deptManagerDf.col(colName = "emp_no") === employeesDf.col(colName = "emp_no")
  employeesDf.join(
    right = deptManagerDf,
    joinExprs = managerJoinCondition,
    joinType = "left_anti")
    .show()

  // find job titles of the 10 best-paid employees
  val mostRecentJobTitlesDf = titlesDf.groupBy(col1 = "emp_no", cols = "title")
    .agg(expr = max(columnName = "to_date").as(alias = "max_to_date"))

  val bestPaidEmployeesDf = employeeSalariesDf
    .orderBy(sortExprs = col(colName = "max_salary").desc)
    .limit(10)

  val bestPaidJobsDf = bestPaidEmployeesDf.join(
    right = mostRecentJobTitlesDf,
    usingColumn = "emp_no")
    .orderBy(sortExprs = col(colName = "max_salary").desc)

  bestPaidJobsDf.show()
}
