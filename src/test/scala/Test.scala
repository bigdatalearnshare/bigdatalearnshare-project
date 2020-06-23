import com.alibaba.druid.util.JdbcConstants
import tech.bigdatalearnshare.alidruid.analyze.SQLParserByAliDruid

/**
  * @Author bigdatalearnshare
  * @Date 2020-09-06
  */
object Test {

  def main(args: Array[String]): Unit = {
    val options = Map(
      "url" -> "jdbc:mysql://localhost:3308/test_db",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root"
    )
    val sql = "select p.name, concat(phone,dep_no) from people p left join dep d on p.name=d.name"
    val dbType = JdbcConstants.MYSQL

    val x = SQLParserByAliDruid.extractDbAndTablesFromSQL(sql, dbType)

    val identifiers = SQLParserByAliDruid.extractDbAndTablesFromSQL(sql, dbType)
      .map(_.identifier)

    val dbTableAndCols = SQLParserByAliDruid.queryTableWithColumns(options, identifiers)

    val createTableSQLs = SQLParserByAliDruid.tableColumnsToCreateTableSql(dbTableAndCols)

    val tableWithQueryColumns = SQLParserByAliDruid.extractTableWithQueryColumns(dbType, sql, createTableSQLs)


    //    Map(people -> Set(phone, name), dep -> Set(dep_no))

    println(tableWithQueryColumns)
  }
}
