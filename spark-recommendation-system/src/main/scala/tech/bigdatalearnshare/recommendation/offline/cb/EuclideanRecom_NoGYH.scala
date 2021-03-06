package tech.bigdatalearnshare.recommendation.offline.cb

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tech.bigdatalearnshare.recommendation.utils.SimilarityAlgorithms

/**
  * @Author bigdatalearnshare
  * @Date 2018-06-02
  */
object EuclideanRecom_NoGYH {
  //每一"组别":("特征因子" -> 用户浏览院校与所属类别中其他院校之间的欧几里得距离)
  def method(views: List[String], data: RDD[Array[String]],
             map: Map[String, (String, String, String, String, String)]):
  RDD[(Array[Double], Double)] = {
    //k:特征因子  v:欧几里得距离

    //获取浏览院校对应的信息
    /*for(view <- views) {
      val viewSchool = map.get(view)
    }*/
    val viewSchool = map.get(views.head).toArray.flatMap(x => Array(x._1, x._2, x._3, x._4, x._5)).map(_.toDouble)

    val rdd = data.map {
      case Array(schoolid, schoolname, location, locationid, school_type, zs, fee, byj) =>
        val features = Array[Double](locationid.toDouble, school_type.toDouble,
          zs.toDouble, fee.toDouble, byj.toDouble)
        //相似度计算欧几里得距离
        val d = SimilarityAlgorithms.euclidean(features, viewSchool)
        features -> d
    }
    rdd
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("euc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //(schoolid,schoolname,location,locationid,school_type,zs,fee,byj)
    val data = sc.textFile("/noClustering.txt").map(_.split(",").map(_.trim)).cache()

    //(schoolid,(locationid, school_type, zs, fee, byj))
    val map = data.map(x => (x(0), (x(3), x(4), x(5), x(6), x(7)))).collect().toMap

    val views = List("2")

    //获取浏览院校与其他院校的欧几里得距离  ("其他院校" -> "欧几里得距离")
    val rdd = method(views, data, map)

    //根据欧几里得距离排序
    val res = rdd.map(x => (x._1.toBuffer, x._2)).sortBy(_._2).collect().drop(1).take(6)
    //打印结果
    res.foreach(println)

    println("===============")
    rdd.map(x => (x._1.toBuffer, x._2)).sortBy(_._2).collect().foreach(println)

    sc.stop()
  }
}
