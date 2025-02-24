/*
En SQL Server Configuration Manager aparece la conexion TCP/IP deshabilitada. Es necesaria que este habilitada para
poder conectarse.
 */

package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._

import java.util.Properties


object MSSQL extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val jdbcUrl = s"jdbc:sqlserver://${args(0)}\\SQLEXPRESS:1433;database=${args(1)}"
    val cxnProp = new Properties()
    cxnProp.put("user", args(3))
    cxnProp.put("password", "")
    cxnProp.put("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val jdbcDF = spark.read.jdbc(jdbcUrl, args(2), cxnProp)
    jdbcDF.show(5)
//    jdbcDF.write.jdbc(jdbcUrl, "", cxnProp)

  }
}
