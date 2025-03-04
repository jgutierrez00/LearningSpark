package Chapters.Chapter5

import Chapters.Chapter
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Windowing extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    implicit val windowingEncoder: Encoder[WindowExercise] = Encoders.product[WindowExercise]

    val dataDS = spark.read
      .format("csv")
      .option("header", "true")
      .option("path", args(0))
      .load().as[WindowExercise]

    dataDS.show()

    /*
    No introducimos una particion porque en la imagen resultado no se aprecia ninguna. La primera apreciacion que se
    ve es un orden por la columna power de forma descendente.
    Si usamos esta condicion como Window y aplicamos las funciones correspondientes, obtenemos el resultado.

    Usando una particion, obtendriamos esos resultados pero solo para cada particion, por ejemplo si usamos como particion
    la columna "empresa", el rank, dense_rank y row_number se harian en sobre cada subgrupo de empresas.

    Si usamos "name" entonces nos sale todo los resultados a 1 ya que no hay nombres iguales por tanto hay un subgrupo
    por cada nombre.
     */
    val windowSpec = Window.orderBy(desc("power"))

    var modifiedDS = dataDS.withColumn("Rank", rank().over(windowSpec))
    modifiedDS = modifiedDS.withColumn("Dense Rank", dense_rank().over(windowSpec))
    modifiedDS = modifiedDS.withColumn("Row Number", row_number().over(windowSpec))

    modifiedDS.show()
  }
}
