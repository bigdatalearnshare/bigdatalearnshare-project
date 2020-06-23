package tech.bigdatalearnshare.recommendation.asyn

import com.fasterxml.jackson.databind.ObjectMapper
import javax.jms.{Message, MessageListener, TextMessage}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import tech.bigdatalearnshare.recommendation.utils.{RedisUtils, SimilarityAlgorithms, SystemProperties}

/**
  * @Author bigdatalearnshare
  * @Date 2018-07-10
  *       监听改变院校数据时,发送的消息({"add/delete/update":"sid"})
  *       如果接收到的sid,在'院校库'查不到相应数据,需要发送通知吗？重新发一遍或告诉我正确的sid,手动调用相应的处理方法？
  **/
class SchoolChangeListener extends MessageListener with Serializable {

  //对象转json,json转对象,直接解析json
  private val MAPPER = new ObjectMapper()

  override def onMessage(message: Message): Unit = {
    //判断消息是否是TextMessage类型
    if (message.isInstanceOf[TextMessage]) {
      //强转为TextMessage
      val textMsg = message.asInstanceOf[TextMessage]
      try {
        //获取消息内容{"sid":1,"type":"add"}
        val json = textMsg.getText
        if (StringUtils.isNotBlank(json)) {
          //解析消息,获取数据
          val jsonNode = MAPPER.readTree(json)
          val _type = jsonNode.get("type").asText()
          val sid = jsonNode.get("sid").asText()
          //加载院校数据
          val schools = SchoolChangeListener.loadData()
          //          val schools = t._1
          //          val sc = t._2

          //根据不同的操作类型,进行不同的处理
          if ("add".equals(_type)) {
            //新增院校时处理
            SchoolChangeListener.add(sid, schools)
          } else if ("delete".equals(_type)) {
            //删除院校时处理
            SchoolChangeListener.delete(sid, schools)
          } else if ("update".equals(_type)) {
            //改变院校属性时处理
            SchoolChangeListener.update(sid, schools)
          }
          //此处关闭SparkContext
          schools.sparkContext.stop()
        }
      } catch {
        //处理ObjectMapper解析json数据时可能产生的异常
        case e: Exception => e.printStackTrace()
      }
    }
  }
}

object SchoolChangeListener {

  private val LOGGER = LoggerFactory.getLogger(classOf[SchoolChangeListener])

  /**
    * 计算院校TopN相似度数据并存储到Redis中
    *
    * @param schoolid 基准院校id
    * @param schools  院校数据(sid,特征因子)
    */
  def computeTopN(schoolid: String, schools: RDD[(String, Array[Double])]): Unit = {
    //根据基准院校id获取院校的特征因子
    val features = schools.collectAsMap()(schoolid) //上面已经判断过schoolid是否存在，这里就不用再判断了
    //计算基准院校与其他院校的相似度,并根据相似度倒序排序
    val resRDD: RDD[(String, Double)] = schools.map { x =>
      //计算基准院校与其他院校的余弦相似度
      val d = SimilarityAlgorithms.cosineSimilarity(x._2, features)
      //返回(其他院校id,对应余弦相似度)
      (x._1, d)
    }.sortBy(-_._2)

    //取相似度TopN如6存储到Redis,注意:去掉自己
    val resTopN = resRDD.collect().slice(1, SystemProperties.OFFLINECB)
    //将TopN院校id和对应的相似度拼接成字符串,方便保存到Redis中    sid1:sims1,sid2:sims2,...
    val sd = resTopN.map(x => x._1 + ":" + x._2).mkString(",")
    //以基准院校id为key,与该院校最相似的TopN院校id和相似度拼成的字符串为value,保存到Redis中
    val key = "recom:offlineCB:" + schoolid
    RedisUtils.set(key, sd)
  }

  /**
    * 新增院校处理
    *
    * @param schoolid 新增院校id
    * @param schools  院校数据(sid,特征因子)
    */
  def add(schoolid: String, schools: RDD[(String, Array[Double])]): Unit = {

    //    val features = schools.collectAsMap()(schoolid)  //schoolid不存在时直接这样获取会抛异常
    val features = schools.collectAsMap().get(schoolid) match {
      case Some(features) => features
      case None => throw new IllegalArgumentException(s"schoolid = $schoolid is not exist or load data is not latest data")
    }

    //1、计算新增院校与其他院校的相似度数据，并存储到Redis中
    computeTopN(schoolid, schools)

    //2、计算其他院校[下称"比较院校"]与新增院校的相似度数据，并存储到Redis中
    schools.foreachPartition { ite =>
      val jedis = RedisUtils.getConnection()
      ite.foreach { it =>
        //根据比较院校id,获取它在Redis中相似度TopN列表:sid1:sims1,sid2:sims2,...
        val key = "recom:offlineCB:" + it._1
        val value = RedisUtils.get(key)
        //value不为空且排除"新增院校"本身
        if (StringUtils.isNotBlank(value) && !schoolid.equals(it._1)) {
          //获取TopN最后一个院校相似度数据
          val arr = value.split(",")
          val lastSim = arr(arr.length - 1).split(":")(1)

          //计算新增院校与比较院校的相似度数据
          val d = SimilarityAlgorithms.cosineSimilarity(it._2, features)
          if (d > lastSim.toDouble) {
            //将新增院校与比较院校的相似度数据 替换掉 比较院校在Redis中最后一个院校的相似度数据
            arr(arr.length - 1) = schoolid + ":" + d
            //拼接比较院校的TopN相似度数据:sid1:sims1,sid2:sims2,...并根据相似度倒序排序
            //将arr元素以逗号间隔拼接比较院校的TopN相似度数据:sid1:sims1,sid2:sims2,...
            val ss = arr.sortBy(_.split(":")(1)).reverse.mkString(",")
            //更新比较院校在Redis中的TopN相似院校数据
            jedis.set(key, ss)
          }
        }
      }
      jedis.close()

    }
  }

  /**
    * 删除院校处理
    *
    * @param schoolid 删除院校id
    * @param schools  院校数据(sid,特征因子) 此时这个数据里面已经没有删除院校的数据
    */
  def delete(schoolid: String, schools: RDD[(String, Array[Double])]): Unit = {
    //  1、清除"删除院校"在Redis中TopN相似度数据
    RedisUtils.delete("recom:offlineCB:" + schoolid)

    /*2、更新Redis中"删除院校"与关联的其他院校相似度数据
    *   1)"删除院校"不在其他院校的TopN列表,不做更新
    *   2)"删除院校"在其他院校的TopN列表中,移除该院校  [有可能移除完啊？虽然实际业务情况不可能TODO]
    */
    schools.foreachPartition { itp =>
      val jedis = RedisUtils.getConnection()
      itp.foreach { it =>
        //获取每个院校的Redis中的TopN列表:sid1:sims1,sid2:sims2,...
        val key = "recom:offlineCB:" + it._1
        val value = jedis.get(key)
        if (StringUtils.isNotBlank(value)) {
          val sims = value.split(",").toBuffer
          //定义索引初始化为-1,确定"删除院校"在其他院校TopN列表中的位置
          //          var i = -1
          //          var j = -1
          var i, j = -1
          sims.foreach { f =>
            i += 1
            val arr = f.split(":")
            if (schoolid.equals(arr(0))) j = i
          }
          //移除"删除院校"在其他院校的TopN列表中
          if (j != -1) {
            sims.remove(j)
            //更新Redis中对应的数据
            val changeValue = sims.mkString(",")
            jedis.set(key, changeValue)
          }
        }
      }
      jedis.close()
    }
  }

  /**
    * 改变院校属性处理
    *
    * @param schoolid 改变属性的院校id
    * @param schools  院校数据(sid,特征因子)
    */
  def update(schoolid: String, schools: RDD[(String, Array[Double])]): Unit = {

    //获取"改变院校"的特征因子
    val features = schools.collectAsMap().get(schoolid) match {
      case Some(features) => features
      case None => throw new IllegalArgumentException(s"schoolid = $schoolid is not exist, please confirm the correct schoolid")
    }

    //  1、更新自己在Redis中的相似度数据
    computeTopN(schoolid, schools)

    //  2、更新其他院校在Redis中的相似度数据
    schools.collect().foreach { f =>
      //获取各个院校的相似度数据:sid1:sims1,sid2:sims2,...
      val key = "recom:offlineCB:" + f._1
      val value = RedisUtils.get(key)
      if (StringUtils.isNotBlank(value) && !schoolid.equals(f._1)) {
        //计算"改变院校"与其他院校的相似度数据
        val d = SimilarityAlgorithms.cosineSimilarity(f._2, features)
        //定义标志:假定"改变院校"不在其他院校的TopN列表里
        var flag = false
        val ss = value.split(",")
        ss.foreach { x =>
          val arr = x.split(":")
          if (schoolid.equals(arr(0))) flag = true
        }
        //获取TopN列表最后一个院校的相似度数据
        val lastSim = ss(ss.length - 1).split(":")(1).toDouble
        if (flag) {
          //在TopN列表
          if (d > lastSim) {
            //(sid,相似度),并根据相似度倒序排序
            val t = ss.map(_.split(":")).map(x => (x(0), x(1).toDouble)).sortBy(-_._2)
            //拼接成sid1:sims1,sid2:sims2,...
            val changeValue = t.map(x => x._1 + ":" + x._2).mkString(",")
            //更新Redis中相应数据
            RedisUtils.set(key, changeValue)
          } else {
            //重新计算其他院校所有的相似度数据倒序取TopN
            computeTopN(f._1, schools)
          }
        } else {
          //不在TopN列表
          if (d > lastSim) {
            //将TopN列表中最后一个院校相似度数据替换为:"改变院校"的相似度数据
            ss(ss.length - 1) = f._1 + ":" + d
            //相似度倒序排序并更新Redis中的数据
            val t = ss.map(_.split(":")).map(x => (x(0), x(1).toDouble)).sortBy(-_._2)
            val changeValue = t.map(x => x._1 + ":" + x._2).mkString(",")
            RedisUtils.set(key, changeValue)
          }
        }
      }
    }
  }

  /**
    * 初始化spark应用环境及加载院校数据
    *
    * @return
    */
  def loadData(): RDD[(String, Array[Double])] = {
    val conf = new SparkConf().setAppName("scl").setMaster("local[*]") //.setMaster("spark://linux-0:7077") //
      .set("spark.executor.memory", "500m")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //本地运行，以免报找不到SchoolChnageProcess
      .setJars(Seq("/Users/ucse/Documents/RecomProject/target/RecomProject-1.0-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //从hive中加载所有院校数据,此时获得已经是最新数据(新增/修改/删除)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("set spark.sql.shuffle.partitions=1") //默认shuffle分区数是20个
    hiveContext.sql("use testdb")
    val df = hiveContext.sql("select * from school2_detail_number_gyh")

    //提取院校的特征因子和院校id
    val schools = df.map {
      case Row(schoolid, schoolname, locationid, school_type, zs, fee, byj) =>
        val features = Array[Double](locationid.toString.toDouble, school_type.toString.toDouble, zs.toString.toDouble, fee.toString.toDouble, byj.toString.toDouble)
        (schoolid.toString, features)
    }.cache()

    //sc.stop()  //SparkContext不要在这里关闭，不然在其他"类"调用并操作schools时会报空指针异常

    //(schools, sc)  //不需要把SparkContext传过去，可以直接通过RDD的sparkContext方法获取创建该RDD的sparkContext
    schools
  }
}
