package Chapters.Chapter3

import Chapters.Chapter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions => F}

object Schemas extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {


    // Leemos un archivo csv y lo guardamos en un dataframe
    val fireDF = spark
      .read
      .option("samplingRatio", 0.001)
      .option("header", true)
      .csv(args(0))

    // println(fireDF.printSchema)

    // Este trozo de codigo es para guardar el DF a un archivo parquet en forma de tabla o forma normal parquet
    //         val parquetPath = "data/__default__/user/current/fireDF.parquet"
    //         val parquetTable = "fireDF"
    //
    //         fireDF.write.format("parquet").save(parquetPath)
    //         fireDF.write.format("parquet").saveAsTable(parquetTable)

    // Algunas transformaciones y acciones
    val fewFireDF = fireDF
      .select("IncidentNumber", "AvailableDtTm", "CallType")
      .where(col("CallType") =!= "Medical Incident")

    fewFireDF.show(5, false)


    // Seleccionamos la columna CallType que no sea null y contamos los valores distintos de la misma
    // agg() se añade cuando tenemos un alias
    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .agg(expr("count(distinct CallType) as DistinctCallTypes"))
      .show()

    fireDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .distinct
      .show(10, false)


    // Renombramos la columna Delay a ResponseDelayedMins
    val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedMins")

    // Mostramos los valores de la columna ResponseDelayedMins que sean mayores a 5
    newFireDF
      .select("ResponseDelayedMins")
      .where(col("ResponseDelayedMins") > 5)
      .show(5, false)

    // Añadimos las columnas IncidentDate, OnWatchDate y AvailableDtTS usando las columnas
    // CallDate, WatchDate y AvailableDtTm respectivamente pero cambiado a tipo timestamp
    val fireTsDF = newFireDF
      .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
      .drop("CallDate")
      .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
      .drop("WatchDate")
      .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
      .drop("AvailableDtTm")

    fireTsDF
      .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
      .show(5, false)

    fireTsDF
      .select(year(col("IncidentDate")))
      .distinct
      .orderBy(year(col("IncidentDate")))
      .show()

    // Mostramos los valores de la columna CallType que no sean null y los agrupamos por CallType
    fireTsDF
      .select("CallType")
      .where(col("CallType").isNotNull)
      .groupBy("CallType")
      .count()
      .orderBy(desc("count"))
      .show(10, false)

    fireTsDF
      .select(F.sum("NumAlarms"), F.avg("ResponseDelayedMins"),
        F.min("ResponseDelayedMins"), F.max("ResponseDelayedMins"))
      .show()
  }
}