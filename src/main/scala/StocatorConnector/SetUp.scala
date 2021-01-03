package StocatorConnector

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

class Setup() {
  val config = ConfigFactory.load()

  val accessKey = config.getString("cos.accessKey")
  val endpoint = config.getString("cos.endpoint")
  val secretKey = config.getString("cos.secretKey")

  def initSpark: SparkSession = {
    SparkSession
      .builder
      .master("local[*]")
      .appName("CosPoc")
      .getOrCreate()
  }

  def initCos(implicit spark: SparkSession): Unit = {
    val hconf = spark.sparkContext.hadoopConfiguration

    hconf.set("fs.stocator.scheme.list", "cos")
    hconf.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    hconf.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    hconf.set("fs.stocator.cos.scheme", "cos")

    hconf.set("fs.cos.myCos.access.key", accessKey)
    hconf.set("fs.cos.myCos.secret.key", secretKey)
    hconf.set("fs.cos.myCos.endpoint", endpoint)
  }

}
