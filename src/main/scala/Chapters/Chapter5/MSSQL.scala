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

    val jdbcUrl = s"jdbc:sqlserver://${args(0)}\\SQLEXPRESS:1433;database=${args(1)};encrypt=false;trustServerCertificate=false;integratedSecurity=true;"

    val jdbcDF = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", args(2))
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .load()

    jdbcDF.show(5)
//    jdbcDF.write.jdbc(jdbcUrl, "", cxnProp)

  }
}
