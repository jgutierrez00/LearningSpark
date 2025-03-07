package Chapters

import org.apache.spark.sql.SparkSession

object App {

  def main(args: Array[String]): Unit = {

    if(args.length == 0){
      println("No se ha pasado un argumento")
      System.exit(1)
    }
    val className = args(0)
    val chapterArgs = args.drop(1)
    var spark: SparkSession = null

    try{
      if(className.equals("Chapter7.Optimization")) {
        spark = SparkSession.builder()
          .appName("LearningSpark")
          .config("spark.sql.shuffle.partitions", "5")
          .config("spark.executor.memory", "2g")
          .master("local[*]")
          .getOrCreate()
      }else if(className.equals("Chapter9.DeltaLake")){
        spark = SparkSession.builder()
          .appName("LearningSpark")
          .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
          .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
          .master("local[*]")
          .getOrCreate()
      }else{
        spark = SparkSession.builder()
          .appName("LearningSpark")
          .master("local[*]")
          .config("spark.executor.memory", "4g")
          .config("spark.driver.memory", "4g")
          .getOrCreate()
      }
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
