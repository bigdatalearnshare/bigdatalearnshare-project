package tech.bigdatalearnshare.recommendation.clustering

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import tech.bigdatalearnshare.recommendation.utils.SystemProperties

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-20
  *       先对院校进行聚类，然后判断浏览院校所属类别，获取该类别内的与该院校相似的院校 【还可以再完善一下】
  */
class KMeansApply {

  def run(): Unit = {
    //windows本地跑，从widnows访问HDFS，需要指定一个合法的身份
    //System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName(SystemProperties.KMEANS_APPNAME).setMaster(SystemProperties.MASTERNAME)
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("set spark.sql.shuffle.partitions=1") //默认shuffle分区数是200个

    //先从hive中加载到日志数据
    hiveContext.sql("use testdb")
    val df = hiveContext.sql("select * from school2_detail_number_gyh")

    //将hive中查询过来的数据，每一条变成一个向量，整个数据集变成矩阵
    val parsedata = df.map { case Row(schoolid, schoolname, locationid, school_type, zs, fee, byj) =>
      //"特征因子":学校位置id,学校类型,住宿方式,学费,备用金
      val features = Array[Double](locationid.toString.toDouble, school_type.toString.toDouble, zs.toString.toDouble, fee.toString.toDouble, byj.toString.toDouble)
      //将数组变成机器学习中的向量
      Vectors.dense(features)
    }.cache() //默认缓存到内存中，可以调用persist()指定缓存到哪

    //用kmeans对样本向量进行训练得到模型
    //聚类中心
    val numclusters = List(3, 6, 9)
    //指定最大迭代次数
    val numIters = List(10, 15, 20)
    var bestModel: Option[KMeansModel] = None
    var bestCluster = 0
    var bestIter = 0
    val bestRmse = Double.MaxValue
    for (c <- numclusters; i <- numIters) {
      val model = KMeans.train(parsedata, c, i)
      //集内均方差总和(WSSSE)，一般可以通过增加类簇的个数 k 来减小误差，一般越小越好（有可能出现过拟合）
      val d = model.computeCost(parsedata)
      println("选择K:" + (c, i, d))
      if (d < bestRmse) {
        bestModel = Some(model)
        bestCluster = c
        bestIter = i
      }
    }
    println("best:" + (bestCluster, bestIter, bestModel.get.computeCost(parsedata)))
    //用模型对我们的数据进行预测
    val resrdd = df.map { case Row(schoolid, schoolname, locationid, school_type, zs, fee, byj) =>
      //提取到每一行的特征值
      val features = Array[Double](locationid.toString.toDouble, school_type.toString.toDouble, zs.toString.toDouble, fee.toString.toDouble, byj.toString.toDouble)
      //将特征值转换成特征向量
      val linevector = Vectors.dense(features)
      //将向量输入model中进行预测，得到预测值
      val prediction = bestModel.get.predict(linevector)

      //返回每一行结果((sid,sname),所属类别)
      ((schoolid.toString, schoolname.toString), prediction)
    }

    //中心点
    /*val centers: Array[linalg.Vector] = model.clusterCenters
    centers.foreach(println)*/

    //按照所属"类别"分组，并根据"类别"排序，然后保存到数据库
    resrdd.groupBy(_._2).sortBy(_._1).foreachPartition(saveData2Mysql(_))

    sc.stop()
  }

  /**
    * 将聚类后的院校数据保存到数据库
    *
    * @param iterator Iterator[(所属类别, Iterable[((sid, sname), 所属类别)])]
    */
  def saveData2Mysql(iterator: Iterator[(Int, Iterable[((String, String), Int)])]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO school_kmeans (schoolid, name, prediction, accesse_date) VALUES (?, ?, ?, ?)"

    try {
      conn = DriverManager.getConnection(SystemProperties.URL, SystemProperties.USERNAME, SystemProperties.PASSWORD)
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(sql)
      iterator.foreach { ite =>
        ite._2.foreach { line =>
          ps.setInt(1, line._1._1.toInt)
          ps.setString(2, line._1._2)
          ps.setInt(3, line._2)
          ps.setDate(4, new Date(System.currentTimeMillis()))
          println("===" + line)
        }
        ps.executeBatch()
        conn.commit()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ps != null) ps.close()
      if (conn != null) conn.close()
    }
  }
}
