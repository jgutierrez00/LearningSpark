package Chapters.Chapter3

import Chapters.Chapter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions => F}

object Schemas extends Chapter{

    override def run(spark: SparkSession, args: Array[String]): Unit = {

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

        val newFireDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedMins")

        newFireDF
            .select("ResponseDelayedMins")
            .where(col("ResponseDelayedMins") > 5)
            .show(5, false)

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