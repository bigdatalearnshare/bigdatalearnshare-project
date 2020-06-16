package tech.bigdatalearnshare.statistics

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.SparkSession

/**
  * @Author bigdatalearnshare
  * @Date 2020-06-14
  */
object IpLocation {

  /** 将ip转换成Long数字. 如1.86.64.0 => 22429696 */
  def ip2Num(ip: String): Long = {
    var ipNum = 0L

    ip.split("[.]")
      .foreach { i =>
        ipNum = i.toLong | ipNum << 8L
      }

    ipNum
  }

  /** ip段中, ip是有序的, 所以采用二分查找法 */
  def binarySearch(ipInfos: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = ipInfos.length - 1

    while (low <= high) {
      val middle = (low + high) / 2

      if ((ip >= ipInfos(middle)._1.toLong) && (ip <= ipInfos(middle)._2.toLong))
        return middle

      if (ip < ipInfos(middle)._1.toLong) high = middle - 1 else low = middle + 1
    }

    -1
  }

  def saveData2MySQL(iterator: Iterator[(String, Int)], batchSize: Int = 1000): Unit = {
    var conn: Connection = null
    var pst: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, ip_count) VALUES (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdatalearnshare", "root", "root")

      // 生产中为了安全起见, 应该首先通过Connection的API获取metadata, 判断事务的支持情况, 失败情况下进行回滚. 这里为了方便, 不在做相应判断
      conn.setAutoCommit(false)
      pst = conn.prepareStatement(sql)

      var rowCount = 0
      iterator.foreach { case (location, count) =>
        pst.setString(1, location)
        pst.setInt(2, count)

        pst.addBatch()
        rowCount += 1

        if (rowCount % batchSize == 0) {
          pst.executeBatch()
          conn.commit()
          rowCount = 0
        }
      }

      if (rowCount > 0) {
        pst.executeBatch()
        conn.commit()
      }

    } catch {
      case e: Exception => println(e)
    } finally {
      if (pst != null) pst.close()
      if (conn != null) conn.close()
    }
  }

  def main(args: Array[String]) {

    val sparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("test")
      .master("local[*]")
      .getOrCreate()

    val ipInfos = sparkSession.read.textFile("/bigdatalearnshare/location/ip.txt").rdd
      .map(_.split("\\|"))
      .map(info => (info(2), info(3), s"${info(6)}-${info(7)}-${info(9)}-${info(13)}-${info(14)}"))
      .collect()

    val ipBroadcast = sparkSession.sparkContext.broadcast(ipInfos)

    val locationIp = sparkSession.read.textFile("/bigdatalearnshare/location/http-format").rdd
      .map(_.split("\\|"))
      .map(_ (1))
      .mapPartitions { iter =>
        val ipInfoVal = ipBroadcast.value
        iter.map { ip =>
          val ipNum = ip2Num(ip)
          val index = binarySearch(ipInfoVal, ipNum)

          (ipInfoVal(index)._3, ip)
        }
      }


    locationIp.map(x => (x._1, 1)).reduceByKey(_ + _)
      .foreachPartition(saveData2MySQL(_))


    sparkSession.stop()
  }
}
