package tech.bigdatalearnshare.utils

import org.springframework.context.support.ClassPathXmlApplicationContext

/**
  * @Author bigdatalearnshare
  * @Date 2020-05-09
  */
object SpringUtils {

  private var context: ClassPathXmlApplicationContext = _

  def getBean(name: String): Any = context.getBean(name)

  def getBean[T](name: String, classObj: Class[T]): T = context.getBean(name, classObj)

  def getBean[T](_class: Class[T]): T = context.getBean(_class)

  def init(springXml: Array[String]): Unit = {
    if (springXml == null || springXml.isEmpty) {
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
