package tech.bigdatalearnshare.utils

import java.io._
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  *
  *       属性文件操作类
  */
object PropertyUtil {

  private val log = LoggerFactory.getLogger(PropertyUtil.getClass)

  private val PROP_CACHE = new ConcurrentHashMap[String, String]()

  /**
    * 从配置文件中获取属性
    *
    * @param fileName 文件名
    * @param key      属性key
    * @return
    */
  def getPropertyFromFile(fileName: String, key: String): String = {
    var result = PROP_CACHE.get(fileName + "_" + key)
    if (result == null) {
      var props: Properties = null
      try {
        //获取已加载配置文件的属性对象
        props = getPros(fileName)
      } catch {
        case e: Exception =>
          log.error("装载文件--->失败!")
          e.printStackTrace()
      }
      if (props == null || props.keySet() == null || props.keySet().size() == 0 || StringUtils.isBlank(props.getProperty(key))) {
        result = null
      } else {
        result = props.getProperty(key).trim
        PROP_CACHE.put(fileName + "_" + key, result)
      }
    }
    result
  }


  /**
    * 获取已加载配置文件的属性对象Properties
    *
    * @param fileName
    * @return
    */
  def getPros(fileName: String): Properties = {
    val properties = new Properties()
    val propName = fileName + ".properties"
    var in: InputStream = null
    try {
      in = PropertyUtil.getClass.getClassLoader.getResourceAsStream(propName)
      if (in == null) {
        in = new FileInputStream(new File(Const.ABS_PATH + propName))
      }
      val bf = new BufferedReader(new InputStreamReader(in, "UTF-8"))
      properties.load(bf)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      in.close()
    }
    properties
  }

  /**
    * 获取Redis配置文件信息
    *
    * @param key
    * @return
    */
  def getRedisPropertyFromFile(key: String): String = {
    getPropertyFromFile("redis", key)
  }

  /**
    * 获取Kafka配置文件信息
    *
    * @param key
    * @return
    */
  def getKafkaPropertyFromFile(key: String): String = {
    getPropertyFromFile("kafka", key)
  }

  /**
    * 获取config.properties配置文件信息
    *
    * @param key
    * @return
    */
  def getConfigPropertyFromFile(key: String): String = {
    getPropertyFromFile("config", key)
  }

  /**
    * 获取jdbc.properties配置文件信息
    *
    * @param key
    * @return
    */
  def getJdbcPropertyFromFile(key: String): String = {
    getPropertyFromFile("jdbc", key)
  }
}
