package tech.bigdatalearnshare.utils

import org.apache.spark.sql.SparkSession

/**
  * @Author fjs
  * @Date 2020-05-04
  */
trait BasicSparkOperation {

  val warehouseLocation = "spark-warehouse"

  //System.setProperty("HADOOP_USER_NAME", "root")

  def getOrCreateSparkSessionWithLocal(): SparkSession = {
    SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("with_local")
      .master("local[*]")
      .getOrCreate()
  }

  def getOrCreateSparkSessionWithHive(): SparkSession = {
    SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .appName("with_hive")
      .master("local[*]")
      .getOrCreate()
  }

}