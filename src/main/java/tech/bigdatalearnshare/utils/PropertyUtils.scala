package tech.bigdatalearnshare.utils

import java.io._
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

/**
  * @Author fjs
  * @Date 2020-05-09
  */
object PropertyUtils {

  private val log = LoggerFactory.getLogger(PropertyUtils.getClass)

  private val PROP_CACHE = new ConcurrentHashMap[String, String]()


  /**
    * 从配置文件中获取属性
    *
    * @param fileName 文件名
    * @param key      属性key
    * @return
    */
  def getPropertyFromFile(fileName: String, key: String) = {
    var value = PROP_CACHE.get(s"${fileName}_$key")

    if (value == null) {
      var props: Properties = null

      try {
        props = load(fileName)
      } catch {
        case e: Exception => throw new CustomException(s"Error load properties from $fileName", e)
      }

      if (props == null || props.keySet() == null || props.keySet().size() == 0 || StringUtils.isBlank(props.getProperty(key))) {
        value = "<unknown>"
      } else {
        value = props.getProperty(key).trim
        PROP_CACHE.put(s"${fileName}_$key", value)
      }
    }

    value
  }

  def load(file: String): Properties = {
    val propName = file + ".properties"

    val info = new Properties()

    var ins: InputStream = null
    try {
      ins = getResourceAsStream(propName)

      info.load(ins)
    } catch {
      case e: Exception => throw new CustomException(s"Error loading properties from $propName", e)
    } finally {
      if (ins != null) try {
        ins.close()
      } catch {
        case e: Exception => throw new CustomException("Error closing resource stream", e)
      }
    }

    info
  }

  @throws[IOException]
  def getResourceAsStream(resourceName: String): InputStream = {
    val cl = Thread.currentThread.getContextClassLoader
    if (cl == null) throw new IOException(s"Can not read resource file $resourceName because class loader of the current thread is null")
    getResourceAsStream(cl, resourceName)
  }

  @throws[IOException]
  def getResourceAsStream(cl: ClassLoader, resourceName: String): InputStream = {
    if (cl == null) throw new IOException(s"Can not read resource file $resourceName because given class loader is null")
    var is = cl.getResourceAsStream(resourceName)

    // 从指定路径下读取配置文件信息
    if (is == null) {
      is = new FileInputStream(new File(ConfigurePath.ABS_PATH + resourceName))
    }

    if (is == null) throw new IOException(s"Can not read resource file $resourceName")

    is
  }

  /** 获取已加载配置文件的属性对象Properties */
  def getPros(fileName: String): Properties = {
    val properties = new Properties()
    val propName = fileName + ".properties"
    var ins: InputStream = null

    try {
      ins = this.getClass.getClassLoader.getResourceAsStream(propName)

      if (ins == null) {
        ins = new FileInputStream(new File(ConfigurePath.ABS_PATH + propName))
      }

      val bf = new BufferedReader(new InputStreamReader(ins, "UTF-8"))

      properties.load(bf)
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (ins != null) try {
        ins.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }

    properties
  }

  /** 获取Redis配置文件信息 */
  def getRedisPropertyFromFile(key: String): String = {
    getPropertyFromFile("redis", key)
  }

  /** 获取Kafka配置文件信息 */
  def getKafkaPropertyFromFile(key: String): String = {
    getPropertyFromFile("kafka", key)
  }

  /** 获取config.properties配置文件信息 */
  def getConfigPropertyFromFile(key: String): String = {
    getPropertyFromFile("config", key)
  }

  /** 获取jdbc.properties配置文件信息 */
  def getJdbcPropertyFromFile(key: String): String = {
    getPropertyFromFile("jdbc", key)
  }

}

/** 获取外部配置文件位置
  *
  * @author fjs
  */
object ConfigurePath {

  val CONF_DIR_NAME = "conf"

  def ABS_PATH = {
    val absolutePath = new File("").getAbsolutePath

    val libIndex = absolutePath.lastIndexOf("lib")

    val endIndex = if (libIndex == -1) 0 else libIndex


    s"${absolutePath.substring(0, endIndex)}${this.CONF_DIR_NAME}${File.separator}"
  }

}

class CustomException(message: String, cause: Throwable) extends Exception(message, cause) {

  def this(message: String) = this(message, null)

}
