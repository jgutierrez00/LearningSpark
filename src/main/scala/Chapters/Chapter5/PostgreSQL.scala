package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._

import java.util.Properties

object PostgreSQL extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // Usando el metodo load
    val jdbcDF1 = spark.read
      .format("jdbc")
      .option("url", s"jdbc:postgresql://${args(0)}:5432/${args(1)}")
      .option("dbtable", args(2))
      .option("user", args(3))
      .option("password", args(4))
      .load()

    jdbcDF1.show()

    // Usando metodo jdbc

    val cxnProp = new Properties()
    cxnProp.put("user", args(3))
    cxnProp.put("password", args(4))

    val jdbcDF2 = spark.read
      .jdbc(s"jdbc:postgresql://${args(0)}/${args(1)}", args(2), cxnProp)

    jdbcDF2.show()

    // Usando metodo save

    jdbcDF1.write
      .format("jdbc")
      .option("url", s"jdbc:postgresql://${args(0)}:5432/${args(1)}")
      .option("dbtable", args(2))
      .option("user", args(3))
      .option("password", args(4))
      .save()

//    jdbcDF2
//      .write
//      .jdbc(s"jdbc:postgresql://${args(0)}", s"${args(1)}.${args(2)}",
//        connectionProperties=new Properties("user": s"${args(3)}", "password": s"${args(4)}")))


  }
}
