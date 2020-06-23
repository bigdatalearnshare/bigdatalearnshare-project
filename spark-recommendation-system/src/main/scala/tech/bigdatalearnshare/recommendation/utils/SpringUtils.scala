package tech.bigdatalearnshare.recommendation.utils

import org.springframework.context.support.ClassPathXmlApplicationContext

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  */
object SpringUtils {

  private var context: ClassPathXmlApplicationContext = _

  // 通过spring获取bean
  def getBean(name: String): Any = context.getBean(name)

  def getBean[T](name: String, classObj: Class[T]): T = context.getBean(name, classObj)

  def getBean[T](_class: Class[T]): T = context.getBean(_class)

  /**
    * 记载spring配置文件并启动服务
    *
    * @param springXml
    */
  def init(springXml: Array[String]): Unit = {
    if (springXml == null || springXml.length == 0) {
      try
        throw new Exception("springXml 不可为空")
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
    context = new ClassPathXmlApplicationContext(springXml(0))
    context.start()
  }
}
