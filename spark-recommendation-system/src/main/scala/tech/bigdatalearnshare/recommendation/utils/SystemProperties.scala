package tech.bigdatalearnshare.recommendation.utils

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  */
object SystemProperties {

  val FORMAT: String = PropertyUtil.getConfigPropertyFromFile("format")

  val DRIVER: String = PropertyUtil.getJdbcPropertyFromFile("jdbc.driver")

  val URL: String = PropertyUtil.getJdbcPropertyFromFile("jdbc.url")

  val USERNAME: String = PropertyUtil.getJdbcPropertyFromFile("jdbc.username")

  val PASSWORD: String = PropertyUtil.getJdbcPropertyFromFile("jdbc.password")

  val TOPICS: String = PropertyUtil.getKafkaPropertyFromFile("topics")

  val GROUP: String = PropertyUtil.getKafkaPropertyFromFile("group")

  val ZK_SERVERS: String = PropertyUtil.getKafkaPropertyFromFile("zk_servers")

  val NUM_THREAADS: String = PropertyUtil.getKafkaPropertyFromFile("numThreads")

  val DURATION: String = PropertyUtil.getKafkaPropertyFromFile("batchDuration")

  val PARTITION_NUM = PropertyUtil.getKafkaPropertyFromFile("partition_num")

  val OFFLINECB: Int = PropertyUtil.getConfigPropertyFromFile("offlineCB.recomNum").toInt

  val ONLINECB: Int = PropertyUtil.getConfigPropertyFromFile("onlineCB.recomNum").toInt

  val OFFLINEALS: Int = PropertyUtil.getConfigPropertyFromFile("offlineALS.recomNum").toInt

  val ONLINEALS: Int = PropertyUtil.getConfigPropertyFromFile("onlineALS.recomNum").toInt

  val OFFLINECB_PREFIX: String = PropertyUtil.getConfigPropertyFromFile("offlineCB.prefix")

  val ONLINECB_PREFIX: String = PropertyUtil.getConfigPropertyFromFile("onlineCB.prefix")

  val OFFLINEALS_PREFIX: String = PropertyUtil.getConfigPropertyFromFile("offlineALS.prefix")

  val ONLINEALS_PREFIX: String = PropertyUtil.getConfigPropertyFromFile("onlineALS.prefix")

  val KAFKA_APPNAME: String = PropertyUtil.getConfigPropertyFromFile("spark.kafkaAppName")

  val KMEANS_APPNAME: String = PropertyUtil.getConfigPropertyFromFile("spark.kmeansAppName")

  val MASTERNAME: String = PropertyUtil.getConfigPropertyFromFile("spark.masterName")

}
