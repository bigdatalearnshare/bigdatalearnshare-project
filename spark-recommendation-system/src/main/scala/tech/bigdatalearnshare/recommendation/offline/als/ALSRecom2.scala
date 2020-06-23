package tech.bigdatalearnshare.recommendation.offline.als

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import tech.bigdatalearnshare.recommendation.utils.{RedisUtils, SystemProperties}

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-24
  */
object ALSRecom2 {

  /** 校验集预测数据和实际数据之间的均方根误差 */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {

    val r = data.map(x => ((x.user, x.product), x.rating))
    //预测数据集
    val prediction = model.predict(data.map(x => (x.user, x.product)))
    //预测数据集和实际数据集进行join
    val predictJoined = prediction.map(x =>
      ((x.user, x.product), x.rating))
      .join(r)
      .values
    //返回均方根误差
    new RegressionMetrics(predictJoined).rootMeanSquaredError
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName("als3").setMaster("local[*]")
      .set("spark.executor.memory", "500m")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    //加载用户对院校的"评分"数据:((1,16,1.15),16)   注意: ( 和 ) 以及 " 切分的时候都需要进行转译
    val ratingData = sc.textFile("/test/school_als_rate_index.txt").
      map(_.replaceAll("\\(", "")).map(_.replaceAll("\\)", "")).map(_.split(","))

    //将"偏好度矩阵"RDD中每一行转换为Rating格式的RDD -MLlib的ALS推荐系统算法目前只支持Rating格式数据集
    val ratings: RDD[(Long, Rating)] = ratingData.map {
      case (Array(userId, schoolId, score, index)) =>
        //(index%10,user,product,rating) - index%10为了后面方便"切分"数据
        (index.toLong % 10, Rating(userId.toInt, schoolId.toInt, score.toDouble))
    }

    //加载院校数据(院校id -> 院校名称)
    val schools = sc.textFile("/test/school.txt").map(_.split(","))
      .map(fields => (fields(0).toInt, fields(1))).collectAsMap()

    //将"评分"数据切分成3个部分,该计算数据在计算过程中多次用到，所以cache到内存
    val numPartitions = 4 //调整并行度
    //训练(60%,加入用户"实时评分")
    val trainData = ratings.filter(x => x._1 < 6)
      .values.repartition(numPartitions)
      .cache()
    //校验(20%)
    val validationData = ratings.filter(x => x._1 >= 6 && x._1 < 8)
      .values.repartition(numPartitions)
      .cache()
    //测试(20%)
    val testData = ratings.filter(x => x._1 >= 8).values.cache()

    //训练不同参数下的模型
    //隐因子个数，越大计算量越大，一般越精准 10-200
    val numRanks = List(10, 12)
    //迭代次数，一般设置为10
    val numIters = List(10, 20)
    //控制正则化过程，值越高正则化程度越高
    val numLambdas = List(0.6, 10.0)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestRmse = Double.MaxValue
    var bestRank = -1
    var bestLambda = -1.0
    var bestIter = 0

    for (rank <- numRanks; iter <- numIters; lambda <- numLambdas) {
      //矩阵分解
      //MatrixFactorizationModel:将结果保存在userFeatures和productFeatures连个RDD里面(user,features),(product,features)
      val model = ALS.train(trainData, rank, iter, lambda)
      val validationRmse = computeRmse(model, validationData)
      println("RMSE(validation)=" + validationRmse + "->ranks=" + rank + "->iter=" + iter + "->lambda=" + lambda)
      if (validationRmse < bestRmse) {
        bestModel = Some(model)
        bestRmse = validationRmse
        bestIter = iter
        bestLambda = lambda
        bestRank = rank
      }
    }

    //每一个用户以及它对应的院校"评分"数据
    val seq = ratings.map(x => (x._2.user, x._2.product)).groupByKey().collect().toSeq
    //排除掉每个用户已"评分"的院校进行推荐
    seq.foreach { x =>
      //uid以及对应的它"评分"的院校
      val uId = x._1
      val viewSchoolIds = x._2.toSeq
      //候选院校
      val candidates = sc.parallelize(schools.keys.filter(!viewSchoolIds.contains(_)).toSeq)
      //进行TopN推荐
      val recoms = bestModel.get.predict(candidates.map(x => (uId, x))).sortBy(-_.rating).take(SystemProperties.OFFLINEALS)
      //按照用户id进行分组
      val res: Map[Int, String] = recoms.map(x => (x.user, x.product))
        .groupBy(_._1)
        .map { x =>
          //将同一用户下推荐的schoolId汇总
          val sids = x._2.map(_._2).mkString(",")
          (x._1, sids)
        }

      //保存到Redis中("recom:offlineALS"+uid,"sid1,sid2,...")
      res.foreach(r => {
        val key = SystemProperties.OFFLINEALS_PREFIX + r._1
        RedisUtils.set(key, r._2)
        println(key, r._2)
      })
      println("=========")
    }
    sc.stop()
  }
}
