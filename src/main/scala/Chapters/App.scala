package Chapters

import Chapters.Chapter1.MnMcount
import Chapters.Chapter2.AuthorsAges
import Chapters.Chapter3.{Schemas, Schemas2}
import Chapters.Chapter4.{Avro, BasicQueries, Binaries, CSV, DataReader, DataWriter, Images, JSON, MetaData, ORC, Parquet, Tables, Views}
import Chapters.Chapter5.{AzureCosmos, MSSQL, MySQL, PostgreSQL, UDFs}
import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]): Unit = {

    if(args.length == 0){
      println("No se ha pasado un argumento")
      System.exit(1)
    }
    val className = args(0)
    val chapterArgs = args.drop(1)

    val spark: SparkSession = SparkSession.builder()
      .appName("LearningSpark")
      .master("local[*]")
      .getOrCreate()

    try{

      val clazz = Class.forName("Chapters."+className + "$")
      val module = clazz.getField("MODULE$").get(null)
      val runMethod = clazz.getMethod("run", classOf[SparkSession], classOf[Array[String]])
      runMethod.invoke(module, spark, chapterArgs)

    }catch{
      case ex: ClassNotFoundException =>
        println(s"Error: La clase ${className} no existe en el paquete Chapters")
        println(ex.printStackTrace())

      case unknowErr: Exception =>
        println(s"Error al ejecutar el job: ${unknowErr.getMessage}")
        println(unknowErr.printStackTrace())
    }finally{
      spark.stop()
    }
  }
}
