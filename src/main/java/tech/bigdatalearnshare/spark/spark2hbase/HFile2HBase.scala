package tech.bigdatalearnshare.spark.spark2hbase

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import tech.bigdatalearnshare.recommendation.utils.ConfProperties
import tech.bigdatalearnshare.spark.spark2hbase.BatchPut2HBase.getOrCreateSparkSessionWithLocal

/**
  * @Author bigdatalearnshare
  * @Date 2020-05-04
  */
object HFile2HBase {

  def main(args: Array[String]): Unit = {
    val sparkSession = getOrCreateSparkSessionWithLocal()

    val rowKeyField = "id"

    val df = sparkSession.read.format("json").load("/people.json")

    val fields = df.columns.filterNot(_ == "id").sorted

    val data = df.rdd.map { row =>
      val rowKey = Bytes.toBytes(row.getAs(rowKeyField).toString)

      val kvs = fields.map { field =>
        val family = Bytes.toBytes("hfile-fy")

        new KeyValue(rowKey, family, Bytes.toBytes(field), Bytes.toBytes(row.getAs(field).toString))
      }

      (new ImmutableBytesWritable(rowKey), kvs)
    }.flatMapValues(x => x).sortByKey()


    val hbaseConf = HBaseConfiguration.create(sparkSession.sessionState.newHadoopConf())
    hbaseConf.set("hbase.zookeeper.quorum", ConfProperties.ZK_SERVERS)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, "hfile")
    val connection = ConnectionFactory.createConnection(hbaseConf)

    val tableName = TableName.valueOf("hfile")

    //没有HBase表则创建
    creteHTable(tableName, connection)

    val table = connection.getTable(tableName)

    try {
      // HBase表 region分布
      val regionLocator = connection.getRegionLocator(tableName)

      val job = Job.getInstance(hbaseConf)

      //设置文件输出的key，生成HFile，OutputKey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])

      //配置HFileOutputFormat2的信息，注意该代码的位置，否则数据能保存到hdfs的hfile_save路径下，但没有导入HBase表中
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator)

      val savePath = "hdfs://linux-1:9000/hfile_save"
      delHdfsPath(savePath, sparkSession)

      job.getConfiguration.set("mapred.output.dir", savePath)

      data.saveAsNewAPIHadoopDataset(job.getConfiguration)


      //将生成的HFile导入到HBase表
      val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
      bulkLoader.doBulkLoad(new Path(savePath), connection.getAdmin, table, regionLocator)

    } finally {
      //WARN LoadIncrementalHFiles: Skipping non-directory hdfs://linux-1:9000/hfile_save/_SUCCESS 不影响,直接把文件移到HBASE对应HDFS地址了
      table.close()
      connection.close()
    }

    sparkSession.stop()
  }


  def creteHTable(tableName: TableName, connection: Connection): Unit = {
    val admin = connection.getAdmin

    if (!admin.tableExists(tableName)) {
      val tableDescriptor = new HTableDescriptor(tableName)
      tableDescriptor.addFamily(new HColumnDescriptor(Bytes.toBytes("hfile-fy")))
      admin.createTable(tableDescriptor)
    }
  }

  def delHdfsPath(path: String, sparkSession: SparkSession) {
    val hdfs = FileSystem.get(sparkSession.sessionState.newHadoopConf())
    val hdfsPath = new Path(path)

    if (hdfs.exists(hdfsPath)) {
      //val filePermission = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.READ)
      hdfs.delete(hdfsPath, true)
    }
  }

}
