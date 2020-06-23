package tech.bigdatalearnshare.recommendation.utils

import java.io.File

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-13
  */
object Const {

  val CONF_FILE_NAME = "conf"

  //主要用来读取外部配置文件
  val ABS_PATH: String = new File("").getAbsolutePath.substring(0, if (new File("").getAbsolutePath.lastIndexOf("lib") == -1) 0
  else new File("").getAbsolutePath.lastIndexOf("lib")) + this.CONF_FILE_NAME + File.separator
}
