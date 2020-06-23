package tech.bigdatalearnshare.recommendation.offline.cb

import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import tech.bigdatalearnshare.recommendation.utils.SystemProperties

/**
  * @Author bigdatalearnshare
  * @Date 2018-06-01
  */
object CosineRecom_NoGYH {

  /**
    * jblas实现余弦相似度
    *
    * @param arr1 Array[Double]
    * @param arr2 Array[Double]
    * @return
    */
  def cosineSimilarity(arr1: Array[Double], arr2: Array[Double]): Double = {
    if (arr1.length != arr2.length) {
      throw new IllegalArgumentException("DoubleMatrixs should have the same length")
    }
    val v1 = new DoubleMatrix(arr1)
    val v2 = new DoubleMatrix(arr2)
    v1.dot(v2) / (v1.norm2() * v2.norm2())
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("cos").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //(schoolid,schoolname,location,locationid,school_type,zs,fee,byj)
    val data = sc.textFile("/noClustering.txt").map(_.split(",").map(_.trim)).cache()

    //(schoolid,(locationid, school_type, zs, fee, byj))
    val map = data.map(x => (x(0), (x(3), x(4), x(5), x(6), x(7)))).collect().toMap

    //RDD里不能套用RDD
    val temp = data.collect()

    //获取每一个院校与其他院校的相似度，并按照相似度进行倒序排序
    temp.foreach { x =>
      //"基准"院校
      val baseSchoolId = x(0)
      val baseFeatures = map.get(baseSchoolId).toArray
        .flatMap(x => Array(x._1, x._2, x._3, x._4, x._5))
        .map(_.toDouble)

      //根据"余弦相似度"排序:余弦相似度值越接近1，越相似
      val resArr: Array[(String, Double)] = temp.map {
        //待比较院校
        case Array(schoolid, schoolname, location, locationid, school_type, zs, fee, byj) =>
          val features = Array[Double](locationid.toDouble, school_type.toDouble,
            zs.toDouble, fee.toDouble, byj.toDouble)
          //计算余弦相似度，保留3位小数
          val d = cosineSimilarity(features, baseFeatures)
          //(比较的院校id,对应的余弦相似度)
          (schoolid, d)
      }.sortBy(-_._2)

      //取最相似的6个院校
      val res: Array[(String, Double)] = resArr.slice(1, SystemProperties.OFFLINECB)
      //将院校id和对应的相似度拼接成字符串  schoolId1:sims1,schoolId2:sims2,...
      val sd = res.map(x => x._1 + ":" + x._2).mkString(",")
      //以每个院校id为key,与该院校最相似的6个院校id和相似度拼成的字符串为value,保存到Redis中
      val key = "recom:offlineCB:" + baseSchoolId
      //RedisUtils.set(key, sd)
      println(key, sd)
    }
    sc.stop()
  }
}
