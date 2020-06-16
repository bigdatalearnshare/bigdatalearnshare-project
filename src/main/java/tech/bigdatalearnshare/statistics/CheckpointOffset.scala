package tech.bigdatalearnshare.statistics

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer

/**
  * @Author bigdatalearnshare
  * @Date 2020-06-13
  */
object CheckpointOffset {
  def main(args: Array[String]): Unit = {
    val path = "/bigdatalearnshare/checkpointLocation/binlog-2-kafka/offsets/2"

    val fs = FileSystem.get(new Configuration())

    val lastFile = fs.listStatus(new Path(path)).filterNot(_.getPath.getName.endsWith(".tmp.crc"))
      .map { fileName =>
        (fileName.getPath.getName.split("/").last.toInt, fileName.getPath)
      }.maxBy(_._1)._2

    val offset = readFile(lastFile.toString).split("\n").last

    assert("2400000001667289".equals(offset))
  }

  def readFile(path: String): String = {
    val fs = FileSystem.get(new Configuration())
    var br: BufferedReader = null
    var line: String = null
    val result = ArrayBuffer.empty[String]
    try {
      br = new BufferedReader(new InputStreamReader(fs.open(new Path(path))))
      line = br.readLine()
      while (line != null) {
        result += line
        line = br.readLine()
      }
    } finally {
      if (br != null) br.close()
    }

    result.mkString("\n")
  }
}