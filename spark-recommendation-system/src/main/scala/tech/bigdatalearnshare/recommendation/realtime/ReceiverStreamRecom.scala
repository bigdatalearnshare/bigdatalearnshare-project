package tech.bigdatalearnshare.recommendation.realtime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import tech.bigdatalearnshare.recommendation.pojo.CusItem
import tech.bigdatalearnshare.recommendation.utils.{ProcessDataUtils, RedisProperties, RedisUtils, SystemProperties}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @Author bigdatalearnshare
  * @Date 2018-06-25
  *
  *       spark-streaming(ssc)和kafka集成Receiver方式进行推荐
  *       batchDuration:如果设置过短，ssc频繁的提交作业使该batchDuration所产生的Job并不能在这期间完成处理，就会造成数据不断堆积，最终导致ssc发生阻塞
  *       根据ssc的可视化监控界面，观察Total Delay来进行batchDuration的调整确定
  *spark.streaming.kafka.maxRatePerPartition:默认是没有上线的，即kafka当中有多少数据它就会直接全部拉出
  *       根据生产者写入Kafka的速率以及消费者本身处理数据的速度，同时这个参数需要结合上面的batchDuration，使得每个partition拉取在每个batchDuration期间拉取的数据能够顺利的处理完毕，
  *       做到尽可能高的吞吐量，而这个参数的调整可以参考可视化监控界面中的Input Rate和Processing Time
  *
  */
object ReceiverStreamRecom {

  /** 处理从kafka接收到的数据
    *
    * @param rdd [(userId,schoolId,score)]
    */
  def processMessages(rdd: RDD[(String, String)]): RDD[(Int, Int, Double)] = {
    //DStream of (Kafka message key, Kafka message value)
    //过滤为""的消息
    val tempMsg = rdd.map(_._2).filter(!_.trim.equals(""))
    //过滤不合法的消息
    val msg = tempMsg.map(_.split(",")).filter(_.length == 3)
    // userId,操作类型,院校id \n userId,操作类型,院校id \n ...
    //按照userId和schoolId进行分组
    val groupRDD = msg.map { case Array(userId, operationType, schoolId) =>
      ((userId, schoolId), operationType)
    }.groupByKey()

    val resRDD = ProcessDataUtils.computeScore(groupRDD)
    resRDD
  }

  /**
    * 实时CB推荐,存储到Redis
    *
    * @param resRDD
    */
  def realContentRecom(resRDD: RDD[(Int, Int, Double)]): Unit = {
    val resCB = resRDD.groupBy(_._1).map { r =>
      val items = new mutable.ListBuffer[CusItem]

      r._2.foreach { case (userId, viewSchoolId, score) =>
        //val t = (schoolId,score)
        //从Redis(或HBASE)中获取当前院校相似度列表
        val value = RedisUtils.get(SystemProperties.OFFLINECB_PREFIX + viewSchoolId)
        if (StringUtils.isNotBlank(value)) {
          val itemStrs = value.split(",")
          for (itemStr <- itemStrs) {
            val arr = itemStr.split(":")
            //院校相似度乘以score作为最终权重
            val cusItem = new CusItem(arr(0).toInt, arr(1).toDouble * score)
            if (!items.contains(cusItem)) {
              items.add(cusItem)
            }
          }
        }
      }
      //根据最终权重进行降序排序
      items.sortWith(_ > _)
      val schoolIds: ListBuffer[Int] = items.map(_.schoolId)
      //        val t = items.map(x => (x.schoolId, x.weight))
      //        println("items+score:" + t)
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

        jedis.close()
      }
    }
  }

  /** 实时ALS推荐,存储到Redis
    *
    * @param resRDD [(userId,view_schoolId,score)]
    */
  def realAlsRecom(resRDD: RDD[(Int, Int, Double)]): Unit = {
    //按照userId分组
    val resALS = resRDD.groupBy(_._1).map { x =>
      val userId = x._1
      //从Redis中获取ALS推荐数据
      val schoolIds = RedisUtils.get(SystemProperties.OFFLINEALS_PREFIX + userId)
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

    /*if (args.length < 4) {
      s"""
         |Usage: DirectStreamRecom <zkQuorum> <topics> <groupId> <numThreads>
         |<zkQuorum> is a list of zookeepers
         |<topics> is a list of one or more kafka topics to consume from
         |<groupid> is a consume group
         |<numThreads> is the number of thread to consume for a topic
         |
         """.stripMargin
      System.exit(1)
    }
    //kafka参数
    //val Array(zkQuorum, topics, groupId, numThreads) = args*/

    /*val conf = new SparkConf().setAppName(this.KAFKA_APPNAME).setMaster(this.MASTERNAME)
      .set("spark.streaming.kafka.maxRatePerPartition", "5") //每个kafka分区最大消费消息数
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf, Seconds(this.DURATION.toLong))
    val Array(zkQuorum, groupId, topics, numThreads) = Array[String](this.ZKQUORUM, this.GROUP, this.TOPICS, this.NUMTHREAADS)
      */

    val conf = new SparkConf().setAppName("rsRecom").setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "5") //每个kafka分区最大消费消息数
      //设置序列化器为KryoSerializer
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //注册要序列化的自定义类型
    //conf.registerKryoClasses(Array(classOf[MyClass1], classOf[MyClass2]))

    //初始化StreamingContext   每隔25秒处理一次【实际待定TODO】
    val ssc = new StreamingContext(conf, Seconds(25))
    val Array(zkQuorum, groupId, topics, numThreads) = Array[String]("linux-1:2181,linux-2:2181,linux-3:2181", "g1", "t1", "2")

    //k:topic,v:线程数
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //返回值DStream of (Kafka message key, Kafka message value)即消息id和消息内容
    val data = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap).cache()

    /*// 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", zkQuorum)
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    data.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        rdd.foreachPartition { records =>
          val producer = kafkaProducer.value
          records.foreach { record =>
            producer.send("topic", "key", "value")
          }
        }
        /*rdd.foreach { record =>
          kafkaProducer.value.send("topic", "key", "value")
        }*/
      }
    }*/
    data.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        //处理消息 (uid,schoolid,score)
        val resRDD = processMessages(rdd)

        //CB实时推荐处理并保存到Redis
        realContentRecom(resRDD)

        //ALS实时推荐处理并保存到Redis
        realAlsRecom(resRDD)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
