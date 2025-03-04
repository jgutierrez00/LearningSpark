package Chapters.Chapter6

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import java.util.Calendar

object Mitigation extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    implicit val personEncoder: Encoder[Person] = Encoders.product[Person]

    val earliestYear = Calendar.getInstance.get(Calendar.YEAR) - 40

    val personDS = spark.read
      .format("json")
      .option("path", args(0))
      .option("multiline", "true")
      .load().as[Person]

    println(personDS
      .filter(x => x.birthDate.split("-")(0).toInt > earliestYear)
      .filter(col("salary") > 80000)
      .filter(x => x.lastName.startsWith("J"))
      .filter(col("firstName").startsWith("D"))
      .count())

    println(personDS
      .filter(year(col("birthDate")) > earliestYear)
      .filter(col("salary") > 80000)
      .filter(col("lastName").startsWith("J"))
      .filter(col("firstName").startsWith("D"))
      .count())
  }
}
