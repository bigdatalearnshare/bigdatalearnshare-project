package tech.bigdatalearnshare.spark.datasource

import tech.bigdatalearnshare.recommendation.utils.BasicSparkOperation

/**
  * @Author bigdatalearnshare
  * @Date 2020-05-15
  */
object BLSExcel extends BasicSparkOperation {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithLocal()

    sparkSession.read.option("useHeader","true").format("com.crealytics.spark.excel").load("/bls_excel.xlsx")
      .show()
  }

}