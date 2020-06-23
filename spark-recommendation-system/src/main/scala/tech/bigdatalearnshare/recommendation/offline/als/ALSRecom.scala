package tech.bigdatalearnshare.recommendation.offline.als

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import tech.bigdatalearnshare.recommendation.utils.{ProcessDataUtils, RedisUtils, SimilarityAlgorithms, SystemProperties}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-22
  */
object ALSRecom {

  /** 校验集预测数据和实际数据之间的均方根误差 */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {

    val r = data.map(x => ((x.user, x.product), x.rating))
    //预测数据集
    val prediction = model.predict(data.map(x => (x.user, x.product)))
    //预测数据集和实际数据集进行join
    val predictJoined = prediction.map(x => ((x.user, x.product), x.rating)).join(r).values
    //返回均方根误差
    new RegressionMetrics(predictJoined).rootMeanSquaredError
  }

  /** 训练不同参数下的模型,获取最优模型 */
  def getBestModel(trainData: RDD[Rating], validationData: RDD[Rating]): Option[MatrixFactorizationModel] = {

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
    bestModel
  }

  def main(args: Array[String]): Unit = {
    //System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf().setAppName("als1").setMaster("local[*]")
      .set("spark.executor.memory", "500m")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
    val resWithIndex = resRDD.zipWithIndex()

    //将"偏好度矩阵"RDD中每一行转换为Rating格式的RDD -MLlib的ALS推荐系统算法目前只支持Rating格式数据集
    val ratings = resWithIndex.map { case (((userId, schoolId, score), index)) =>
      //(index%10,user,product,rating) - index%10为了后面方便"切分"数据
      (index.toLong % 10, Rating(userId.toInt, schoolId.toInt, score.toDouble))
    }
    //加载院校数据(院校id -> 院校名称)
    val schools = sc.textFile("/test/school.txt").map(_.split(","))
      .map(fields => (fields(0).toInt, fields(1))).collectAsMap()

    //将"评分"数据切分成3个部分,该计算数据在计算过程中多次用到，所以cache到内存
    val numPartitions = 4 //调整并行度
    //训练(60%,加入用户"实时评分")
    val trainData = ratings.filter(x => x._1 < 6).values.repartition(numPartitions).cache()
    //校验(20%)
    val validationData = ratings.filter(x => x._1 >= 6 && x._1 < 8).values.repartition(numPartitions).cache()
    //测试(20%)
    val testData = ratings.filter(x => x._1 >= 8).values.cache()

    //获取最优模型
    val bestModel = getBestModel(trainData, validationData)

    //    //保存训练好的模型
    //    bestModel.get.save(sc,"hdfs://linux-1:9000/als/model")
    //    //加载模型
    //    val model = MatrixFactorizationModel.load(sc,"hdfs://linux-1:9000/als/model")

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
      res.foreach { r =>
        val key = SystemProperties.OFFLINEALS_PREFIX + r._1
        RedisUtils.set(key, r._2)
        println(key, r._2)
      }
      println("=========")
    }
    sc.stop()
  }
}

object ApplyModel {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("als-model").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val model = MatrixFactorizationModel.load(sc, "hdfs://linux-1:9000/als/model")
    """
      |根据用户推荐物品
      |根据用户推荐用户
      |根据物品推荐物品
    """.stripMargin
    println("---------基于用户推荐------------")
    val data = sc.textFile("hdfs://linux-1:9000/data/data.txt")
    //schoolID name
    val map = data.map(_.split("\\|").take(2)).map(x => (x(0).toInt, x(1))).collectAsMap()

    //给111用户推荐10个院校
    """
      |为单个用户推荐商品  指定用户和推荐个数
    """.stripMargin
    val uid = 111
    val n = 10
    val recomTops = model.recommendProducts(uid, n)
    println(recomTops.mkString(","))

    """
      |给所有用户推荐商品  指定推荐个数
    """.stripMargin
    //给所有用户进行推荐
    model.recommendProductsForUsers(n).flatMap { x =>
      val user = x._1
      val ratings = x._2
      val res = ListBuffer[(String, String, String)]()
      ratings.foreach { r =>
        res.add(user.toString, r.product.toString, map(r.product) + "," + r.rating)
      }
      res
    }

    """
      |给商品推荐用户  指定商品ID|推荐用户个数
    """.stripMargin
    model.recommendUsers(451, n)

    """
      |给所有商品推荐用户  指定推荐个数
    """.stripMargin
    model.recommendUsersForProducts(n)

    """
      |根据物品推荐物品
    """.stripMargin
    """
      |根据某物品推荐物品
      |基于物品推荐物品的,可以将推荐结果事先保存到es中,大数据系统直接取检索,即可得到推荐的结果
      |推荐每天更新一次
    """.stripMargin
    val ss: String = similarProduct(model, 230, 10).toList.mkString(":::")
    println(ss)
    """
      |找到所有物品的最类似的物品
      |与本身最相近的还是本身,所以如果取前10个,N需要设置为11
    """.stripMargin
    map.map { tuple =>
      val recomList = similarProduct(model, tuple._1, 10)
      (tuple._1, recomList.toList.mkString(":::"))
    } //.foreach(println)
  }

  /**
    * 得到用户因子和物品因子
    */
  def obtainFeatures(model: MatrixFactorizationModel): (RDD[(Int, Array[Double])], RDD[(Int, Array[Double])]) = {
    val userFeatures = model.userFeatures
    val productFeatures = model.productFeatures
    (userFeatures, productFeatures)
  }

  /**
    * 求某个物品与各个物品的余弦相似度
    */
  def productCosineSimilarity(model: MatrixFactorizationModel, itemId: Int): RDD[(Int, Double)] = {
    val itemFactor = model.productFeatures.lookup(itemId).head
    val itemVector = new DoubleMatrix(itemFactor)

    val sims = model.productFeatures.map {
      case (id, factor) =>
        val factorVector = new DoubleMatrix(factor)
        val sim = SimilarityAlgorithms.cosineSimJblas(factorVector, itemVector)
        (id, sim)
    }
    sims
  }


  /**
    * 求与某个物品相似的前N的物品
    */
  def similarProduct(model: MatrixFactorizationModel, itemId: Int, N: Int = 10): Array[(Int, Double)] = {
    val sims = this.productCosineSimilarity(model, itemId)
    val sortedSims = sims.top(N)(Ordering.by { x => x._2 })
    sortedSims
  }
}
