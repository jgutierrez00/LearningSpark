package Chapters.Chapter6

import Chapters.Chapter
import org.apache.spark.sql._

object CaseClasses extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    implicit val bloggersEncoder: Encoder[Bloggers] = Encoders.product[Bloggers]

    val bloggersDS = spark.read
      .format("json")
      .option("path", args(0))
      .load().as[Bloggers]

    bloggersDS.show()
  }
}
