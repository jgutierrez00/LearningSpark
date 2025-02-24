package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._

object UDFs extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val cubed = (s: Long) => {
      s * s * s
    }

    spark.udf.register("cubed", cubed)

    spark.range(1,9).createOrReplaceTempView("udf_test")

    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

  }
}
