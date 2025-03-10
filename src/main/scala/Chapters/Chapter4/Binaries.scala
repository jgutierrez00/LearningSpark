package Chapters.Chapter4

import Chapters.Chapter
import org.apache.spark.sql._

object Binaries extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val path = args(0)

    // Cargamos los archivos binarios en un DataFrame
    val binaryFilesDF = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .load(path)

    // Cargamos los archivos binarios de forma recursiva en un DataFrame
    val binaryFilesDFRec = spark.read.format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .option("recursiveFileLookup", "true")
      .load(path)

    binaryFilesDF.show(5)
    binaryFilesDFRec.show(5)

  }
}
