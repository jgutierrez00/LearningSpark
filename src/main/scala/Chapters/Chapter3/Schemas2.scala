package Chapters.Chapter3

import Chapters.Chapter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Schemas2 extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {


    // Definimos un esquema y creamos un DataFrame con él
    val schema = StructType(Array(StructField("Id", IntegerType, false),
      StructField("First", StringType, false),
      StructField("Last", StringType, false),
      StructField("Url", StringType, false),
      StructField("Published", StringType, false),
      StructField("Hits", IntegerType, false),
      StructField("Campaigns", ArrayType(StringType), false)))

    val blogsDF = spark.read.schema(schema).json(args(0))
    blogsDF.show(false)

    println(blogsDF.printSchema)
    println(blogsDF.schema)
  }
}
