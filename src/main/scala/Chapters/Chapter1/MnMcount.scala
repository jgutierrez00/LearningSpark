package Chapters.Chapter1

import Chapters.Chapter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount extends Chapter  {

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    println("Iniciando MnMcount...")

    if(args.length == 0){
      print("No files as arguments")
      System.exit(1)
    }

    // Sacamos el fichero de los argumento, lo leemos y lo pasamos
    // a un DataFrame.
    val mnmFile = args(0)
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    /**
     Se cuentan con aggregate todos los colores y se agrupan por State y Color
     Orderby los ordena.
     */
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Se calcula y se muestra el resultado para las 60 primeras filas?
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()


    /**
     Mismo caso que el anterior solo que en este filtramos antes por estado
     */
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    caCountMnMDF.show(10)

  }
}
