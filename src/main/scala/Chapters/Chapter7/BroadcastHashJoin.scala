package Chapters.Chapter7

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions.broadcast

object BroadcastHashJoin extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val person = spark.read.json(args(0))
    val country = spark.read.json(args(1))

    if (person.isEmpty || country.isEmpty) {
      println("One of the dataframes is empty. Exiting.")
      return
    }
    val joinExpr = person.col("country") === country.col("name")

    val joinType = "inner"

    val joinResult = person.join(broadcast(country), joinExpr, joinType)

    joinResult.show()
  }
}
