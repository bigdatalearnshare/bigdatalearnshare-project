package org.apache.spark.sql

/**
  * @Author bigdatalearnshare
  * @Date 2020-09-06
  */
case class BLSTableIdentifier(table: String, database: Option[String]) {
  val identifier: String = table

  def this(table: String) = this(table, None)
}