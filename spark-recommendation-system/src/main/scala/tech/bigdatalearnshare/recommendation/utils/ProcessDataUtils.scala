package tech.bigdatalearnshare.recommendation.utils

import org.apache.spark.rdd.RDD

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-19
  */
object ProcessDataUtils {
  /**
    * 以userId和schoolId为组，统计每个用户对不同院校的不同操作类型(浏览/收藏/评论等)，计算最终score
    *
    * @param rdd [((userId, schoolId),operationType)]
    */
  def computeScore(rdd: RDD[((String, String), Iterable[String])]): RDD[(Int, Int, Double)] = {
    val resRDD = rdd.map { ite =>
      //统计用户对院校的操作类型的次数
      //      var views = 0
      //      var collect = 0
      //      var evaluate = 0
      var views, collect, evaluate = 0
      ite._2.foreach { case (operationType) =>
        if (operationType == "浏览") views += 1
        if (operationType == "收藏") collect += 1
        if (operationType == "评价") evaluate += 1
        //        operationType match {
        //          case "浏览" => views += 1
        //          case "收藏" => collect += 1
        //          case "评价" => evaluate += 1
        //          case _ =>
        //        }
      }
      //假设：浏览(+15%)/收藏(+55%)/评价(+30%)  基础权重为1，每浏览/收藏/评价一次，加相应的权重比
      val score = views * (1 + 0.15) + collect * (1 + 0.55) + evaluate * (1 + 0.3)
      val userId = ite._1._1.toInt
      val schoolId = ite._1._2.toInt
      (userId, schoolId, score)
    }
    resRDD
  }
}
