package tech.bigdatalearnshare.recommendation.utils

import org.slf4j.LoggerFactory
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  */
object RedisUtils {

  private val log = LoggerFactory.getLogger(RedisUtils.getClass)

  private var REDISEXPIRE: Integer = _

  @volatile private var jedis: Jedis = _
  private var jedisPool: JedisPool = _

  /**
    * 初始化JedisPoll
    *
    * @return
    */
  def init(): JedisPool = {
    if (jedisPool == null) {
      synchronized {
        if (jedisPool == null) {
          val config = new JedisPoolConfig()

          //设置Redis参数
          config.setMaxTotal(RedisProperties.MAX_TOTAL)
          config.setMaxIdle(RedisProperties.MAX_IDLE)
          config.setMaxWaitMillis(RedisProperties.MAX_WAITMILLS)
          config.setMinEvictableIdleTimeMillis(RedisProperties.MIN_EvictableIdleTimeMillis)
          config.setTimeBetweenEvictionRunsMillis(RedisProperties.TIME_BetweenEvictionRunsMillis)
          config.setNumTestsPerEvictionRun(RedisProperties.NUM_TestsPerEvictionRun)
          config.setTestOnBorrow(RedisProperties.TestOnBorrow)

          //过期时间:默认7天
          REDISEXPIRE = RedisProperties.EXPIRE * 24 * 60 * 60

          jedisPool = new JedisPool(config, RedisProperties.HOST, RedisProperties.PORT, RedisProperties.TIMEOUT, RedisProperties.PASSWORD)
        }
      }
    }
    jedisPool
  }

  /**
    * 获取Jedis实例
    *
    * @return
    */
  def getConnection(): Jedis = {
    //lazy懒加载:此处不会实例化，当后面再调用jedisPool时才会实例化且只实例化一次
    lazy val jedisPool = init()

    try {
      jedis = jedisPool.getResource
    } catch {
      case e: Exception =>
        log.error("failed get jedis", e)
        if (jedis != null) {
          jedisPool.returnBrokenResource(jedis)
        }
    }
    jedis
  }

  def set(key: Array[Byte], value: Array[Byte]): Unit = {
    val jedis = getConnection()
    jedis.set(key, value)
    jedis.close()
  }

  def setex(key: Array[Byte], value: Array[Byte]): Unit = {
    val jedis = getConnection()
    jedis.setex(key, REDISEXPIRE, value)
    jedis.close()
  }

  def set(key: String, value: String): Unit = {
    val jedis = getConnection()
    jedis.set(key, value)
    jedis.close()
  }

  def setex(key: String, value: String): Unit = {
    val jedis = getConnection()
    jedis.setex(key, REDISEXPIRE, value)
    jedis.close()
  }

  def get(key: String): String = {
    val jedis = getConnection()
    val result = jedis.get(key)
    jedis.close()
    result
  }

  def get(key: Array[Byte]): Array[Byte] = {
    val jedis = getConnection()
    val result = jedis.get(key)
    jedis.close()
    result
  }


  def delete(key: String): Unit = {
    val jedis = getConnection()
    jedis.del(key)
    jedis.close()
  }

}