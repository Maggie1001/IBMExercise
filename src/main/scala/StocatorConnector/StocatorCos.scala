package StocatorConnector

import com.ibm.db2.cmx.runtime.internal.repository.api.DataManager
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.FloatType

object StocatorCos {

  def main(args:Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val config = ConfigFactory.load()

    val setup = new Setup()
    implicit val spark:SparkSession = setup.initSpark

    setup.initCos

    val log = LogManager.getRootLogger
    //log.setLevel(Level.ERROR)
    log.setLevel(Level.INFO)
    log.info("start of thing")

    val dataManager = new DataManager()

    log.info("Read CSV File in COS bucket")
    val cosPath = "cos://candidate-exercise.myCos/emp-data.csv"
    val dfData1 = dataManager.dataRead(cosPath)
    dfData1.show(15)

    log.info("Write the Data to DB2 User Table")
    dataManager.dataWriteDb2(dfData1)

    log.info("Read Data From DB2 User Table")
    val dataFromDb2 = dataManager.dataReadDb2()
    //dataFromDb2.show(5)

    log.info("Show the Gender Ratio of Each Department")
    val genderRatioDf = dataManager.genderRatioDep(dataFromDb2)

    log.info("Process the Original Column Salary and Calculated Average Salary of Each Department")
    val updatedDf = dataFromDb2.withColumn(
        "Salary",
         regexp_replace(col("Salary"),
        "[,]|[$]" ,
        "")
        .cast(FloatType))

    val avgSalaryDf = dataManager.avgSalaryDep(updatedDf)

    log.info("Calculated Salary Gap Between Gender of Each Department")
    val salaryGapDf = dataManager.salaryGapDep(updatedDf)

    log.info("Write the Gender Ratio of Each Department as Parquet to COS Bucket")
    val writeCosPath = "cos://candidate-exercise.myCos/genderRatioFinal.parquet"
    val file = "parquet"
    dataManager.dataWriteCos(genderRatioDf, writeCosPath, file)

    log.info("Finished!")

/*
    val readParquetData = spark.read.parquet("cos://candidate-exercise.myCos/genderRatioTest2.parquet")
    readParquetData.show(2)
*/

  }


}
