package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object HighOrderFunctions extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)

    val tempDF = spark.createDataFrame(Seq(
      (1, t1),
      (2, t2)
    )).toDF("id", "celsius")

    tempDF.show()

    tempDF.createOrReplaceTempView("tempDF")

    // funcion transform

    spark.sql(
        """SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
          |FROM tempDF""".stripMargin)
      .show()

    // funcion filter

    spark.sql(
        """SELECT celsius, filter(celsius, t -> t > 38) as high
          |FROM tempDF""".stripMargin)
      .show()

    // funcion exists

    spark.sql(
        """SELECT celsius, exists(celsius, t -> t = 38) as threshold
          |FROM tempDF""".stripMargin)
      .show()

    // funcion reduce no funciona

//    spark.sql(
//        """SELECT celsius, reduce(celsius, 0, (t, acc) -> t + acc, acc -> (acc div size(celsius))) as avgTemp
//          |FROM tempDF""".stripMargin)
//      .show()


  }
}
