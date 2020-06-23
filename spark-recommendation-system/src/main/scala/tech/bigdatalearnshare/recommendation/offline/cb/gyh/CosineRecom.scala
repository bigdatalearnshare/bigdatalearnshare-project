package tech.bigdatalearnshare.recommendation.offline.cb.gyh

import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import tech.bigdatalearnshare.recommendation.utils.{RedisUtils, SystemProperties}

/**
  * @Author bigdatalearnshare
  * @Date 2018-06-09
  */
object CosineRecom {

  /**
    * jblas实现余弦相似度
    *
    * @param arr1 Array[Double]
    * @param arr2 Array[Double]
    * @return
    */
  def cosineSimilarity(arr1: Array[Double], arr2: Array[Double]): Double = {
    //数组长度必须相等
    require(arr1.length == arr1.length, s"SimilarityAlgorithms:Array length do not match: Len(x)=${arr1.length} and Len(y)" +
      s"=${arr1.length}.")

    val v1 = new DoubleMatrix(arr1)
    val v2 = new DoubleMatrix(arr2)
    v1.dot(v2) / (v1.norm2() * v2.norm2())
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cos").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
    hiveContext.sql("set spark.sql.shuffle.partitions=1") //默认shuffle分区数是200个

    //先从hive中加载到日志数据  数据已进行归一化处理
    hiveContext.sql("use testdb")
    val df = hiveContext.sql("select * from school2_detail_number_gyh")

    val data = df.map { case Row(schoolid, schoolname, locationid, school_type, zs, fee, byj) =>
      (schoolid.toString, (locationid.toString, school_type.toString, zs.toString, fee.toString, byj.toString))
    }.cache()

    val temp = data.collect()
    //方便根据院校ID获取对应的特征因子
    val dataMap = temp.toMap

    //获取每一个院校与其他院校的相似度，并按照相似度进行倒序排序
    temp.foreach { x =>
      //RDD里不能套用RDD
      //"基准"院校
      val baseSid = x._1
      val baseFeatures = dataMap.get(baseSid)
        .toArray
        .flatMap(x => Array(x._1, x._2, x._3, x._4, x._5))
        .map(_.toDouble)
      //根据"余弦相似度"倒序排序:余弦相似度值越接近1，越相似
      val resArr = temp.map { case (schoolid, (locationid, school_type, zs, fee, byj)) =>
        val features = Array[Double](locationid.toDouble, school_type.toDouble, zs.toDouble, fee.toDouble, byj.toDouble)
        //计算相似度
        //            val d = SimilarityAlgorithms.euclidean(features,baseFeatures)
        //            val d = SimilarityAlgorithms.pearsonCorrelationSimilarity(features,baseFeatures)
        val d = cosineSimilarity(features, baseFeatures)

        //(比较的院校id,对应的余弦相似度)
        (schoolid, d)
      }.sortBy(-_._2)

      //取最相似的6个院校
      val res: Array[(String, Double)] = resArr.slice(1, SystemProperties.OFFLINECB)
      //将院校id和对应的相似度拼接成字符串  schoolId1:sims1,schoolId2:sims2,...
      val sd = res.map(x => x._1 + ":" + x._2).mkString(",")
      //以每个院校id为key,与该院校最相似的6个院校id和相似度拼成的字符串为value,保存到Redis中
      val key = SystemProperties.OFFLINECB_PREFIX + baseSid
      RedisUtils.set(key, sd)
      println(key, sd)
    }
    sc.stop()
  }
}
