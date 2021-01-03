package StocatorConnector

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

class DataManager()(implicit spark: SparkSession) {

  val config = ConfigFactory.load()
  val db2Url = config.getString("db2.db2Url")
  val tableName = config.getString("db2.tableName")
  val user = config.getString("db2.user")
  val password = config.getString("db2.password")
  val driver = config.getString("db2.driver")

  def dataRead(path: String):DataFrame = {
    spark.read.option("header", true).csv(path)
  }

  def dataWriteDb2(df:DataFrame):Unit = {
    df.write.format("jdbc")
      .mode("overwrite")
      .option("url", db2Url)
      .option("dbtable", tableName)
      .option("user", user)
      .option("password", password)
      .save()
  }

  def dataReadDb2(): DataFrame = {
    val dataReadFromDb2 = spark.read
      .format("jdbc")
      .option("url", db2Url)
      .option("driver", driver)
      .option("user", user)
      .option("password", password)
      .option("dbtable", tableName)
      .load()
    dataReadFromDb2
  }

  def genderRatioDep(df: DataFrame):DataFrame = {
    df.createOrReplaceTempView("tempTable")
    val genderRatio = spark.sql("select Department, round(sum(case when Gender = 'Female' then 1 else 0 end)/count(*),2) as FemaleRatio," +
      s"round(sum(case when Gender = 'Male' then 1 else 0 end)/count(*),2) as MaleRatio from tempTable group by Department")
    genderRatio.show()
    genderRatio
  }

  def avgSalaryDep(df: DataFrame) : DataFrame = {
    df.createOrReplaceTempView("tempTable")
    val avgSalary = spark.sql(s"select Department, round(avg(Salary), 2) as AvgSalary from tempTable group by Department")
    avgSalary.show()
    avgSalary
  }

  // Other method to write genderRatioDep instead of sql query
  /*
  def avgSalaryDep2(df: DataFrame) : DataFrame = {
    val avgSalary2 = df.withColumn(
        "Salary",
         regexp_replace(col("Salary"),
        "[,]|[$]" ,
        "")
        .cast(FloatType))
        .groupBy("Department")
        .agg(avg(col("Salary")))
        .withColumnRenamed("AverageSalary2")
    avgSalary2.show()
    avgSalary2
  }
   */

  def salaryGapDep(df: DataFrame): DataFrame = {
    df.createOrReplaceTempView("tempTable")
    val salaryGap = spark.sql("with temp as (select Department, sum(case when Gender = 'Female' then Salary end) as FemaleSalary," +
      s"sum(case when Gender = 'Male' then Salary end) as MaleSalary from tempTable group by Department) " +
      "select temp.Department, temp.FemaleSalary-temp.MaleSalary as SalaryGapFemaleToMale from temp")
    salaryGap.show()
    salaryGap
  }

  def dataWriteCos(df:DataFrame, path: String, fileType: String) = {
    df.coalesce(1).write.format(fileType).mode("overwrite").parquet(path)
  }

}
