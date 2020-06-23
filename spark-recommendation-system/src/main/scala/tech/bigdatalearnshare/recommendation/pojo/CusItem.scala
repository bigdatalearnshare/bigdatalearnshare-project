package tech.bigdatalearnshare.recommendation.pojo

/**
  * @Author bigdatalearnshare
  * @Date 2018-05-22
  */
class CusItem(val schoolId: Int, val weight: Double) extends Ordered[CusItem] with Serializable {

  override def compare(that: CusItem): Int = {
    if (this.weight - that.weight != 0) {
      (this.weight - that.weight).toInt
    } else {
      0
    }
  }

  override def equals(obj: scala.Any): Boolean = {
    //如果对象为null返回false
    if (obj == null) {
      false
    }
    /*obj match {
      case cusItem: CusItem =>
      val that = obj.asInstanceOf[CusItem]
        //如果schoolId一样,则视为同一对象
        this.schoolId == that.schoolId
    }*/
    //判断是否属于CusItem类型
    if (obj.isInstanceOf[CusItem]) {
      val that = obj.asInstanceOf[CusItem]
      //如果schoolId一样,则视为同一对象
      this.schoolId == that.schoolId
    }
    super.equals(obj)
  }

}
