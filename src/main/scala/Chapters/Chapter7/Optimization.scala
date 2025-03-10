package Chapters.Chapter7

import org.apache.spark.sql._
import Chapters.Chapter

object Optimization extends Chapters.Chapter {

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    printConfigs(spark)

    // Configuramos el número de particiones para el shuffle
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)

    spark.sql("SET -v").select("key", "value").show(5, false)

  }

  def printConfigs(spark: SparkSession): Unit = {

    val mconf = spark.conf.getAll

    for(k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n") }
  }
}
