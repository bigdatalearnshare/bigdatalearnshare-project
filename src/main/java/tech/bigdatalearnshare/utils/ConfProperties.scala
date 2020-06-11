package tech.bigdatalearnshare.utils

/**
  * @Author bigdatalearnshare
  * @Date 2020-05-09
  */
object ConfProperties {

  val DRIVER: String = PropertyUtils.getJdbcPropertyFromFile("jdbc.driver")

  val URL: String = PropertyUtils.getJdbcPropertyFromFile("jdbc.url")

  val USERNAME: String = PropertyUtils.getJdbcPropertyFromFile("jdbc.username")

  val PASSWORD: String = PropertyUtils.getJdbcPropertyFromFile("jdbc.password")

  val ZK_SERVERS: String = PropertyUtils.getConfigPropertyFromFile("zk_servers")

  val BROKERS: String = PropertyUtils.getConfigPropertyFromFile("kafka_brokers")

  //val ZK_PORT: String = PropertyUtils.getConfigPropertyFromFile("zk_port")

  val JOBNAME: String = PropertyUtils.getConfigPropertyFromFile("jobName")

  val MASTER: String = PropertyUtils.getConfigPropertyFromFile("master")

  val FORMAT: String = PropertyUtils.getConfigPropertyFromFile("format")

  val hdfs_path: String = PropertyUtils.getConfigPropertyFromFile("hdfs_path")

}
