package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object DataWriter extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val location = args(0)

    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), ("Denny", 31),
      ("Jules", 30), ("TD", 35))).toDF("name", "age")

    dataDF.write.format("json").mode("overwrite").save(location)
  }
}
