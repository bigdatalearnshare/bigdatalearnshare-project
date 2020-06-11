package tech.bigdatalearnshare.alidruid.analyze

import com.alibaba.druid.sql.SQLUtils
import com.alibaba.druid.sql.repository.SchemaRepository
import com.alibaba.druid.sql.visitor.SchemaStatVisitor
import com.alibaba.druid.util.JdbcConstants
import org.apache.spark.sql.BLSTableIdentifier

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * @Author bigdatalearnshare
  * @Date 2020-09-06
  */
object SQLParserByAliDruid {

  // 1. extract db and table by parse sql
  def extractDbAndTablesFromSQL(sql: String, dbType: String = JdbcConstants.MYSQL) = {
    val schemaRepository = new SchemaRepository(dbType)
    schemaRepository.console(sql)

    val statement = SQLUtils.parseSingleStatement(sql, dbType)
    val visitor = new SchemaStatVisitor()

    statement.accept(visitor)

    visitor.getTables.asScala.map { ts =>
      val dbAndTable = ts._1.getName
      if (dbAndTable.contains(".")) {
        val Array(dbName, tableName) = dbAndTable.split("\\.", 2)
        BLSTableIdentifier(tableName, Option(dbName))
      } else {
        BLSTableIdentifier(dbAndTable, None)
      }
    }.toList

  }

  // 2. query all columns and datatype
  def queryTableWithColumns(options: Map[String, String], tables: List[String]) = {
    val dbTableAndCols = mutable.HashMap.empty[String, mutable.HashMap[String, String]]

    val driver = options("driver")
    val url = options("url")

    Class.forName(driver)
    val connection = java.sql.DriverManager.getConnection(url, options("user"), options("password"))

    try {
      val metaData = connection.getMetaData

      tables.foreach { table =>
        val rs = metaData.getColumns(null, null, table, "%")

        val colAndDataTypes = dbTableAndCols.getOrElse(table, mutable.HashMap.empty[String, String])

        while (rs.next()) {
          colAndDataTypes += (rs.getString("COLUMN_NAME") -> rs.getString("TYPE_NAME"))
        }

        dbTableAndCols.update(table, colAndDataTypes)

        rs.close()
      }

    } finally {
      if (connection != null) connection.close()
    }

    dbTableAndCols
  }

  // 3. construct create table sql
  def tableColumnsToCreateTableSql(dbTableAndCols: mutable.HashMap[String, mutable.HashMap[String, String]]) = {
    var createTableSQLs = List.empty[String]

    dbTableAndCols.foreach { t =>
      val createSql = s"create table ${t._1} (  ${t._2.map(m => s"${m._1}  ${m._2}").mkString(",")}  );"

      createTableSQLs :+= createSql
    }

    createTableSQLs
  }

  // 4. extract table with query columns
  def extractTableWithQueryColumns(dbType: String, sql: String, createTableSQLs: List[String]) = {
    val tableWithQueryCols = mutable.HashMap.empty[String, mutable.HashSet[String]]

    val repository = new SchemaRepository(dbType)
    createTableSQLs.foreach(repository.console)

    //repository.resolve(sql) convert * to specific column names,
    //eg: db.table have columns [id,name], then convert sql 'select * from db.table' to 'select id,name from db.table'
    val stmt = SQLUtils.parseSingleStatement(repository.resolve(sql), dbType)

    repository.resolve(stmt)

    val statVisitor = SQLUtils.createSchemaStatVisitor(dbType)
    stmt.accept(statVisitor)

    val iter = statVisitor.getColumns.iterator()
    while (iter.hasNext) {
      val column = iter.next()
      val (table, colName) = (column.getTable, column.getName)

      if (column.isSelect && !"UNKNOWN".equals(table)) {
        val value = tableWithQueryCols.getOrElse(table, mutable.HashSet.empty[String])
        value.add(colName)
        tableWithQueryCols.update(table, value)
      }
      //      else if ("UNKNOWN".equals(tableName)) {
      //        throw new RuntimeException(s"unknown tableName:$tableName")
      //      }
    }

    tableWithQueryCols
  }

}