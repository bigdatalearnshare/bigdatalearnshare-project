package tech.bigdatalearnshare.recommendation.offline.als

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tech.bigdatalearnshare.recommendation.utils.ProcessDataUtils

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-24
  *       离线数据的预处理，统计出(uid,sid,score)
  */
object ALSRecom1 {

  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName("als1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //加载数据
    val data = sc.textFile("/test/school_als.txt").map(_.split(","))

    //根据userId和schoolId进行分组
    val groupRDD: RDD[((String, String), Iterable[String])] = data.map {
      case Array(userId, operationType, schoolId) =>
        ((userId, schoolId), operationType)
    }.groupByKey()

    //统计(userId,schoolId,score)
    val resRDD = ProcessDataUtils.computeScore(groupRDD)

    //给data中每一条数据添加"索引"，方便后续根据"索引"切分数据    zipWithIndex如何过牵涉到要shuffle的RDD则不保证每个分区内都有序
    val resWithIndex: RDD[((Int, Int, Double), Long)] = resRDD.zipWithIndex()
    resWithIndex.sortBy(_._1._1).collect().foreach(println)
    //    resRDD.sortBy(_._1).collect().foreach(println)
    println("===========")
    resWithIndex.collect().foreach(println)
    sc.stop()
  }
}
