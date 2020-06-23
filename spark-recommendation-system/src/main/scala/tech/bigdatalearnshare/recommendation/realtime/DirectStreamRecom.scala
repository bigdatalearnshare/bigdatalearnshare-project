package tech.bigdatalearnshare.recommendation.realtime

import kafka.serializer.StringDecoder
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaManager
import org.apache.spark.streaming.{Seconds, StreamingContext}
import tech.bigdatalearnshare.recommendation.pojo.CusItem
import tech.bigdatalearnshare.recommendation.utils.{RedisProperties, RedisUtils, SystemProperties}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * @Author bigdatalearnshare
  * @Date 2018-06-21
  *       spark-streaming和kafka集成Direct方式进行推荐实现
  */
object DirectStreamRecom {

  /** 处理从kafka接收到的数据
    *
    * @param rdd [(userId,schoolId,score)]
    */
  def processMessages(rdd: RDD[(String, String)]): RDD[(Int, Iterable[(Int, Int, Double)])] = {
    //DStream of (Kafka message key, Kafka message value)
    val tempMsg = rdd.map(_._2).filter(!_.trim.equals(""))
    //过滤不合法的消息
    val msg = tempMsg.map(_.split(",")).filter(_.length == 3)
    // userId,操作类型,院校id \n userId,操作类型,院校id \n ...
    //按照userId和schoolId进行分组
    val groupRDD = msg.map { case Array(userId, operationType, schoolId) =>
      ((userId, schoolId), operationType)
    }.groupByKey()

    //统计(userId,schoolId,score),并根据userId进行分组
    val resRDD: RDD[(Int, Iterable[(Int, Int, Double)])] = groupRDD.map { ite =>
      //统计用户对院校的操作类型的次数
      var views = 0
      var collect = 0
      var evaluate = 0
      ite._2.foreach { case (operationType) =>
        if (operationType == "浏览") views += 1
        if (operationType == "收藏") collect += 1
        if (operationType == "评价") evaluate += 1
      }
      //假设：浏览(+15%)/收藏(+55%)/评价(+30%)  基础权重为0，每浏览/收藏/评价一次，加相应的权重比
      val score = views * 0.15 + collect * 0.55 + evaluate * 0.3
      val userId = ite._1._1.toInt
      val schoolId = ite._1._2.toInt

      (userId, schoolId, score)
    }.groupBy(_._1)
    resRDD
  }

  /** 实时CB推荐
    *
    * @param resRDD [(userId,schoolId,score)]
    * @return
    */
  def realContentRecom(resRDD: RDD[(Int, Iterable[(Int, Int, Double)])]): Unit = {

    val resCB = resRDD.map { r =>
      //定义集合存储带有推荐院校id属性的自定义类CusItem
      val items = new mutable.ListBuffer[CusItem]
      r._2.foreach { case (userId, baseSchoolId, score) =>
        //从Redis(或HBASE)中获取当前院校相似度列表
        val value = RedisUtils.get("recom:offlineCB:" + baseSchoolId)
        if (StringUtils.isNotBlank(value)) {
          val itemStrs = value.split(",")
          for (itemStr <- itemStrs) {
            val arr = itemStr.split(":")
            //院校相似度乘以score作为最终权重
            val cusItem = new CusItem(arr(0).toInt, arr(1).toDouble * score)
            if (!items.contains(cusItem)) items.add(cusItem)
          }
        }
      }
      //根据最终权重进行降序排序
      items.sortWith(_ > _)
      val schoolIds = items.map(_.schoolId)
      /*//返回推荐院校列表id  ,拼接成字符串利于存储到Redis
      var schoolIds = ""
      items.foreach(x => {
        schoolIds += x.schoolId + ","
      })
      schoolIds*/
      //(userId,相应推荐列表)
      (r._1, schoolIds)
    }
    //将CB实时推荐结果保存到Redis
    resCB.foreachPartition { fp =>
      val jedis = RedisUtils.getConnection()
      fp.foreach { f =>
        val expire = RedisProperties.EXPIRE * 24 * 60 * 60
        val value = f._2.mkString(",")
        jedis.setex(SystemProperties.ONLINECB_PREFIX + f._1, expire, value)
      }
      jedis.close()
    }
  }

  /** 实时ALS推荐
    *
    * @param resRDD [(userId,schoolId,score)]
    */
  def realAlsRecom(resRDD: RDD[(Int, Iterable[(Int, Int, Double)])]): Unit = {

    val resALS = resRDD.map { x =>
      val userId = x._1
      //从Redis中获取ALS推荐数据
      val schoolIds = RedisUtils.get("recom:offlineALS:" + userId)
      if (StringUtils.isNotBlank(schoolIds)) {
        (userId.toString, schoolIds)
      } else {
        //设置一些默认推荐
        ("", "")
      }
    }

    //将ALS实时推荐结果保存到Redis
    resALS.foreachPartition { fp =>
      val jedis = RedisUtils.getConnection()
      fp.foreach { f =>
        val expire = RedisProperties.EXPIRE * 24 * 60 * 60
        jedis.setex(SystemProperties.ONLINEALS_PREFIX + f._1, expire, f._2) //f._2已经是用,分隔好的推荐院校id
      }
      jedis.close()
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      s"""
         |Usage: DirectStreamRecom <brokers> <topics> <groupId>
         |<brokers> is a list of one or more Kafka brokers
         |<topics> is a list of one or more kafka topics to consume from
         |<groupid> is a consume group
         |
         """.stripMargin
      System.exit(1)
    }
    //初始化StreamingContext  每隔5秒处理一次【实际待定TODO】
    val Array(brokers, topics, groupId) = args
    val conf = new SparkConf().setAppName("dsRecom").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "5") //每个kafka分区最大消费消息数
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(5))

    //设置kafka参数
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest")
    val km = new KafkaManager(kafkaParams)
    //创建消息流
    val directKafkaStream = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet).cache()

    //    directKafkaStream.transform { t =>
    //      t.map(x => (x._1, x._2))//还是RDD，最终返回一个DStream
    //    }

    directKafkaStream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        //先处理消息 (uid,schoolid,score)
        val resRDD = processMessages(rdd)

        //CB推荐列表
        realContentRecom(resRDD)

        //ALS推荐列表
        realAlsRecom(resRDD)

        //再更新offsets
        km.updateZKOffsets(rdd)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}