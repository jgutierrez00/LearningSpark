package Chapters.Chapter5

import Chapters.Chapter

import org.apache.spark.sql._

object MySQL extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // Carga de datos con load
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://${args(0)}:3306/${args(1)}")
      .option("driver", "com.mysql.jdbc,Driver")
      .option("dbtable", args(2))
      .option("user", args(3))
      .option("password", args(4))
      .load()

    // Guardado de datos con save

    jdbcDF.write
      .format("jdbc")
      .option("url", s"jdbc:mysql://${args(0)}:3306/${args(1)}")
      .option("driver", "com.mysql.jdbc,Driver")
      .option("dbtable", args(2))
      .option("user", args(3))
      .option("password", args(4))
      .save()



  }
}
