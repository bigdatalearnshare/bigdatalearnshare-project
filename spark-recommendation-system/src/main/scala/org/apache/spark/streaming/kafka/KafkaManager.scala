package org.apache.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.{Err, LeaderOffset}

import scala.reflect.ClassTag

/**
  * @Author bigdatalearnshare
  * @Date 2018-06-21
  *       Spark-Streaming和Kafka直连方式:自己管理offsets
  */
class KafkaManager(val kafkaParams: Map[String, String]) extends Serializable {

  private val kc = new KafkaCluster(kafkaParams)

  /**
    * 创建数据流
    *
    * @param ssc
    * @param kafkaParams
    * @param topics
    * @tparam K
    * @tparam V
    * @tparam KD
    * @tparam VD
    * @return
    */
  def createDirectStream[
  K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K] : ClassTag,
  VD <: Decoder[V] : ClassTag](ssc: StreamingContext,
                               kafkaParams: Map[String, String],
                               topics: Set[String]): InputDStream[(K, V)] = {
    //获取消费者组id
    val groupId = kafkaParams.get("group.id").get

    //从zookeeper上读取offset前先根据实际情况更新offset
    setOrUpdateOffsets(topics, groupId)

    //从zookeeper上读取offset开始消费message
    val messages = {
      //获取分区      //Either处理异常的类，通常Left表示异常，Right表示正常
      val partitionsE: Either[Err, Set[TopicAndPartition]] = kc.getPartitions(topics)
      if (partitionsE.isLeft) throw new SparkException(s"get kafka partition failed:${partitionsE.left.get}")

      val partitions = partitionsE.right.get
      //获取消费offsets
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) throw new SparkException(s"get kafka consumer offsets failed:${consumerOffsetsE.left.get}")

      val consumerOffsets = consumerOffsetsE.right.get
      //创建流
      KafkaUtils.createDirectStream[K, V, KD, VD, (K, V)](ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[K, V]) => (mmd.key, mmd.message))
    }
    messages
  }

  /**
    * 创建数据流之前，根据实际情况更新消费offsets
    *
    * @param topics
    * @param groupId
    */
  def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach { topic =>
      var hasConsumed = true
      //获取每一个topic分区
      val partitionsE = kc.getPartitions(Set(topic))
      if (partitionsE.isLeft) throw new SparkException(s"get kafka partition failed:${partitionsE.left.get}")

      //正常获取分区结果
      val partitions = partitionsE.right.get
      //获取消费偏移量
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false

      if (hasConsumed) {
        //消费过
        /**
          * 如果streaming程序执行时出现kafak.common.OffsetOutofRangeException
          * 说明zk上保存的offsets已经过时即kafka定时清理策略已经将包含该offsets的文件删除
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets大小
          * 如果consumerOffsets比earliestLeaderOffsets小，说明consumerOffsets已过时
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          *
          * 在一个group是新的consumer group时，即首次消费，zk上还没有相应的group offsets目录，
          * 这时要先初始化一下zk上的offsets目录，或者是zk上记录的offsets已经过时，
          * 由于kafka有定时清理策略，直接从zk上的offsets开始消费会报ArrayOutofRange异常，即找不到offsets所属的index文件
          *
          * 问题：从consumer offsets到leader latest offsets中间延迟了很多消息，
          * 在下一次启动的时候，首个batch要处理大量的消息，可能导致spark-submit设置的资源无法满足大量消息的处理而导致崩溃。
          * 因此在spark-submit启动的时候加一个配置:--conf spark.streaming.kafka.maxRatePerPartition=10000。或者在代码中控制SparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10000")
          * 限制每秒钟从topic的每个partition最多消费的消息条数，这样就把首个batch的大量的消息拆分到多个batch中去了，
          * 为了更快的消化掉delay的消息，可以调大计算资源和把这个参数调大
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft) throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")

        val earliestLeaderOffsets: Map[TopicAndPartition, KafkaCluster.LeaderOffset] = earliestLeaderOffsetsE.right.get
        val consumerOffsets: Map[TopicAndPartition, Long] = consumerOffsetsE.right.get
        //可能只是部分分区consumerOffsets过时，只更新过时分区的consumerOffsets为earliestLeaderOffsets
        //        TopicAndPartition(topic,partition) Long:consumerOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach { case (tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition + "offsets已过时，更新为:" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        }
        if (!offsets.isEmpty) kc.setConsumerOffsets(groupId, offsets)
        //        val cs = consumerOffsetsE.right.get
        //        val lastest = kc.getLatestLeaderOffsets(partitions).right.get
        //        val earliest = kc.getEarliestLeaderOffsets(partitions).right.get
        //        var newCS: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
        //        cs.foreach { f =>
        //          val max = lastest.get(f._1).get.offset
        //          val min = earliest.get(f._1).get.offset
        //          newCS += (f._1 -> f._2)
        //          //如果zookeeper中记录的offset在kafka中不存在（已过期）就指定其现有kafka的最小offset位置开始消费
        //          if (f._2 < min) {
        //            newCS += (f._1 -> min)
        //          }
        //          println(max + "-----" + f._2 + "--------" + min)
        //        }
        //        if (newCS.nonEmpty) kc.setConsumerOffsets(groupId, newCS)
      } else {
        //没有消费过
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)

        val leaderOffsets: Map[TopicAndPartition, LeaderOffset] = if (reset == Some("smallest")) {
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft) throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsetsE.right.get
        } else {
          //largest
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft) throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map { case (tp, lo) => (tp, lo.offset) }
        kc.setConsumerOffsets(groupId, offsets)

        /*
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
          (tp, lo.offset)
      }
    }
        */

      }
    }
  }

  /**
    * 更新zookeeper上的消费offsets
    *
    * @param rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams("group.id")
    val offsetList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    offsetList.foreach { offset =>
      val topicAndPartition = TopicAndPartition(offset.topic, offset.partition)
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offset.untilOffset)))
      if (o.isLeft) println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
    }
  }
}
