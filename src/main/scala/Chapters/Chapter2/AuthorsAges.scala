package Chapters.Chapter2

import Chapters.Chapter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

/**
    En esta clase se usan Dataframes desde cero
*/
object AuthorsAges extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    // Creamos un dataframe al cual le pasamos los valores
    val dataDF = spark.createDataFrame(Seq(("Brooke", 20), ("Brooke", 25), ("Denny", 31),
      ("Jules", 30), ("TD", 35))).toDF("name", "age")

    // Usamos la funcion avg que hemos importado para ver la edad media, agrupandolo por nombres.
    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    avgDF.show()
    }
}