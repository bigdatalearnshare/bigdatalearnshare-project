package tech.bigdatalearnshare.spark.spark2hbase

import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import tech.bigdatalearnshare.utils.{BasicSparkOperation, ConfProperties}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * @Author bigdatalearnshare
  * @Date 2020-05-04
  */
object BatchPut2HBase extends BasicSparkOperation {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithLocal()

    // 仅为测试使用，不考虑row key设计
    val rowKeyField = "id"

    val df = sparkSession.read.format("json").load("/stats.json")

    val fields = df.columns.filterNot(_ == "id")


    df.rdd.foreachPartition { partition =>
      val hbaseConf = HBaseConfiguration.create()
      hbaseConf.set("hbase.zookeeper.quorum", ConfProperties.ZK_SERVERS)
      hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "batch_put")

      val conn = ConnectionFactory.createConnection(hbaseConf)
      val table = conn.getTable(TableName.valueOf("batch_put"))

      val res = partition.map { row =>
        val rowKey = Bytes.toBytes(row.getAs(rowKeyField).toString)
        val put = new Put(rowKey)
        val family = Bytes.toBytes("hfile-fy")

        fields.foreach { field =>
          put.addColumn(family, Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
        }

        put
      }.toList

      Try(table.put(res)).getOrElse(table.close())

      table.close()
      conn.close()
    }
    /*完全跟关系型数据库中设计一致，但会带来较大的数据冗余(KeyValue结构化开销)，
          可以考虑将business、source、domain合并，year、month、day合并，visitType、visitType各一列，
          pv、uv、ip、regisUsers、applyUsers、testUsers合并，ctime单独一列*/

    sparkSession.stop()
  }

}
