package tech.bigdatalearnshare.spark.spark2hbase

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import tech.bigdatalearnshare.spark.spark2hbase.BatchPut2HBase.getOrCreateSparkSessionWithLocal
import tech.bigdatalearnshare.utils.ConfProperties

/**
  * @Author bigdatalearnshare
  * @Date 2020-05-04
  */
object Direct2HBase {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithLocal()

    val hbaseConf = sparkSession.sessionState.newHadoopConf()
    hbaseConf.set("hbase.zookeeper.quorum", ConfProperties.ZK_SERVERS)

    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "direct")
    val job = Job.getInstance(hbaseConf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])


    val rowKeyField = "id"

    val df = sparkSession.read.format("json").load("/stats.json")

    val fields = df.columns.filterNot(_ == "id")


    df.rdd.map { row =>
      val put = new Put(Bytes.toBytes(row.getAs(rowKeyField).toString))

      val family = Bytes.toBytes("hfile-fy")

      fields.foreach { field =>
        put.addColumn(family, Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
      }

      (new ImmutableBytesWritable(), put)

    }.saveAsNewAPIHadoopDataset(job.getConfiguration)


    sparkSession.stop()
  }

}
