
object TestIp {

  def ip2Num(ip: String): Long = {
    //用.分割ip
    val fragments = ip.split("[.]")
    var ipNum = 0L
    //循环ip的4个数
    //    for (i <- 0 until fragments.length) {
    //      ipNum = fragments(i).toLong | ipNum << 8L
    //    }

    fragments.indices.foreach { index =>
      val x =  fragments(index).toLong | ipNum << 8L

      ipNum = x
    }

    ipNum
  }


  def main(args: Array[String]): Unit = {
    val ip = "1.86.64.0"

    println(ip2Num(ip))

    // 1969608581


    println("0".toLong | 87360 << 8L)

    println("2".toLong)
  }

}
