package tech.bigdatalearnshare.recommendation.utils

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  */
object RedisProperties {

  val HOST: String = PropertyUtil.getRedisPropertyFromFile("redis.host")

  val PORT: Int = PropertyUtil.getRedisPropertyFromFile("redis.port").toInt

  val PASSWORD: String = PropertyUtil.getRedisPropertyFromFile("redis.password")

  val TIMEOUT: Int = PropertyUtil.getRedisPropertyFromFile("redis.timeout").toInt

  val MAX_TOTAL: Int = PropertyUtil.getRedisPropertyFromFile("redis.maxTotal").toInt

  val MAX_IDLE: Int = PropertyUtil.getRedisPropertyFromFile("redis.maxIdle").toInt

  val MAX_WAITMILLS: Int = PropertyUtil.getRedisPropertyFromFile("redis.maxWaitMillis").toInt

  val MIN_EvictableIdleTimeMillis: Int = PropertyUtil.getRedisPropertyFromFile("redis.minEvictableIdleTimeMillis").toInt

  val TIME_BetweenEvictionRunsMillis: Int = PropertyUtil.getRedisPropertyFromFile("redis.timeBetweenEvictionRunsMillis").toInt

  val NUM_TestsPerEvictionRun: Int = PropertyUtil.getRedisPropertyFromFile("redis.numTestsPerEvictionRun").toInt

  val TestOnBorrow: Boolean = PropertyUtil.getRedisPropertyFromFile("redis.testOnBorrow").toBoolean

  //  val EXPIRE: Integer = if (PropertyUtil.getRedisPropertyFromFile("redis.expire") == null) 6 else Integer.parseInt(PropertyUtil.getRedisPropertyFromFile("redis.expire"))
  val EXPIRE: Integer = if (PropertyUtil.getRedisPropertyFromFile("redis.expire") == null) 6 else PropertyUtil.getRedisPropertyFromFile("redis.expire").toInt
}
