package tech.bigdatalearnshare.recommendation.clustering

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tech.bigdatalearnshare.recommendation.utils.SimilarityAlgorithms

import scala.collection.mutable

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-19
  *       先对院校进行聚类，然后判断浏览院校所属类别，获取该类别内的与该院校相似的院校 【还可以再完善一下】
  */
object ClusteringRecom {

  //每一"组别":("特征因子" -> 用户浏览院校与所属类别中其他院校之间的欧几里得距离)
  def method(viewSchoolId: String, data: RDD[Array[String]],
             map1: Map[String, String],
             map2: Map[String, (String, String, String, String, String)]):
  RDD[mutable.HashMap[Array[Double], Double]] = {

    var map = new mutable.HashMap[Array[Double], Double]()

    val rdd = data.map {
      case Array(schoolid, schoolname, location, locationid, school_type, zs, fee, byj, prediction) =>
        (prediction, (schoolid, locationid, school_type, zs, fee, byj))
    }.groupByKey.map { lines =>
      //判断用户浏览的院校id所属"组别"
      val x: Option[String] = map1.get(viewSchoolId)
      val str: String = x match {
        case Some(a) => a
        case None => "no"
      }
      if (lines._1.equals(str)) {
        //根据用户浏览院校id获取院校相应数据
        val tuple: Option[(String, String, String, String, String)] = map2.get(viewSchoolId)
        //(locationid, school_type, zs, fee, byj)
        val view = tuple.toArray.flatMap(x => Array(x._1, x._2, x._3, x._4, x._5)).map(_.toDouble)

        //对每一"组别"进行操作
        lines._2.map { case (schoolid, locationid, school_type, zs, fee, byj) =>
          val features = Array[Double](locationid.toDouble, school_type.toDouble,
            zs.toDouble, fee.toDouble, byj.toDouble)
          //相似度计算：欧几里得
          val d = SimilarityAlgorithms.euclidean(view, features)
          //              map += (((features,d),d));map += ((features, d) -> d)
          map += (features -> d)
        }
      }
      map
    }
    rdd
  }

  def main(args: Array[String]): Unit = {
    //设置环境和加载数据
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //(schoolid,schoolname,location,locationid,school_type,zs,fee,byj,prediction)
    val data: RDD[Array[String]] = sc.textFile("/ceshi.txt").map(_.split(",").map(_.trim)).cache()

    //(schoolid,prediction) -->方便判断"院校"所属"类别"
    val map1 = data.map(x => (x(0), x(8))).collect().toMap

    //(schoolid,(locationid, school_type, zs, fee, byj))
    val map2 = data.map(x => (x(0), (x(3), x(4), x(5), x(6), x(7)))).collect().toMap

    //根据浏览院校id，获取与它属于同一类别的院校"特征因子"和欧几里得距离
    val viewSchoolId = "1"

    //以浏览院校为基准,"特征因子"->"欧几里得距离"
    val rdd = method(viewSchoolId, data, map1, map2)


    val sortRDD: RDD[(Array[Double], Double)] = rdd.flatMap { m =>
      //将map转为Seq以便根据欧几里得距离进行排序:升序  （"特征因子","-距离"作为相似度：待定）
      m.map(x => (x._1, x._2)).toList.sortBy(_._2)
    }
    //移除自己,最终推荐3个。剩余不足3个全取出来
    val t: Array[(Array[Double], Double)] = sortRDD.collect()
    val tuples = if (t.length >= 3) t.slice(1, 4) else if (0 < t.length && t.length < 3) t.drop(1) else t
    val v = tuples.map(x => (x._1.toBuffer, x._2))
    v.foreach(println)

    sc.stop()
  }
}
