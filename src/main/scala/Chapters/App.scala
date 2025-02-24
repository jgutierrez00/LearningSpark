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
      .config("spark.jars.packages", "databricks:spark-deep-learning:1.5.0-spark2.4-s_2.11")
      .getOrCreate()

    try{

      className match {
        case "Chapter1.MnMcount" =>
          MnMcount.run(spark, chapterArgs)
        case "Chapter2.AuthorsAges" =>
          AuthorsAges.run(spark)
        case "Chapter3.Schemas" =>
          Schemas.run(spark, chapterArgs)
        case "Chapter3.Schemas2" =>
          Schemas2.run(spark, chapterArgs)
        case "Chapter4.BasicQueries" =>
          BasicQueries.run(spark, chapterArgs)
        case "Chapter4.Tables" =>
          Tables.run(spark, chapterArgs)
        case "Chapter4.Views" =>
          Views.run(spark, chapterArgs)
        case "Chapter4.MetaData" =>
          MetaData.run(spark, chapterArgs)
        case "Chapter4.DataReader" =>
          DataReader.run(spark, chapterArgs)
        case "Chapter4.DataWriter" =>
          DataWriter.run(spark, chapterArgs)
        case "Chapter4.Parquet" =>
          Parquet.run(spark, chapterArgs)
        case "Chapter4.JSON" =>
          JSON.run(spark, chapterArgs)
        case "Chapter4.CSV" =>
          CSV.run(spark, chapterArgs)
        case "Chapter4.Avro" =>
          Avro.run(spark, chapterArgs)
        case "Chapter4.ORC" =>
          ORC.run(spark, chapterArgs)
        case "Chapter4.Images" =>
          Images.run(spark, chapterArgs)
        case "Chapter4.Binaries" =>
          Binaries.run(spark, chapterArgs)
        case "Chapter5.UDFs" =>
          UDFs.run(spark)
        case "Chapter5.PostgreSQL" =>
          PostgreSQL.run(spark, chapterArgs)
        case "Chapter5.MySQL" =>
          MySQL.run(spark, chapterArgs)
        case "Chapter5.AzureCosmos" =>
          AzureCosmos.run(spark, chapterArgs)
        case "Chapter5.MSSQL" =>
          MSSQL.run(spark, chapterArgs)
      }
    }catch{
      case ex: ClassNotFoundException =>
        println(s"Error: La clase ${className} no existe en el paquete Chapters")
        println(ex.printStackTrace())

      case unknowErr: Exception =>
        println(s"Error al ejecutar el job: ${unknowErr.getMessage}")
        println(unknowErr.toString())
    }finally{
      spark.stop()
    }
  }
}
