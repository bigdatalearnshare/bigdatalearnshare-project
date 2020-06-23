package tech.bigdatalearnshare.recommendation.start

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.slf4j.LoggerFactory
import tech.bigdatalearnshare.recommendation.producer.ProducerJ
import tech.bigdatalearnshare.recommendation.utils.{Const, SpringUtils}

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  */
object Bootstrap {

  private val log = LoggerFactory.getLogger(Bootstrap.getClass)


  def main(args: Array[String]): Unit = {
    init()
  }


  /**
    * 记载Spring配置文件和log4j
    */
  private def init(): Unit = {
    try {
      SpringUtils.init(Array[String]("applicationContext-send.xml"))
      val producer = SpringUtils.getBean("producerJ", classOf[ProducerJ])
      producer.work()
      //initLog4j();
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def initLog4j(): Unit = {
    val fileName = Const.ABS_PATH + "log4j.properties"
    if (new File(fileName).exists) {
      PropertyConfigurator.configure(fileName)
      log.info("日志log4j已经启动")
    }
  }
}