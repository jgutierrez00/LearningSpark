package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._

object UDFs extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // Creamos una funcion UDF
    val cubed = (s: Long) => {
      s * s * s
    }

    // Registramos la funcion UDF
    spark.udf.register("cubed", cubed)

    // Creamos un DataFrame
    spark.range(1,9).createOrReplaceTempView("udf_test")

    // Usamos la funcion UDF
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()

  }
}
