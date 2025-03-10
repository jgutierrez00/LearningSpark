package Chapters.Chapter6

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import scala.util.Random

object SampleData extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    implicit val usageEnconder: Encoder[Usage] = Encoders.product[Usage]

    val r = new Random(42)

    // Create a Dataset of Usage objects
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000)))

    val dsUsage = spark.createDataset(data).as[Usage]
    dsUsage.show()

    dsUsage.filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    dsUsage.map(u => {computeCostUsage(u.usage)})(Encoders.scalaDouble)
      .show(5, false)
  }

  def computeCostUsage(usage: Int): Double = {
    if (usage > 750) usage * .15 else usage * .50
  }
}
