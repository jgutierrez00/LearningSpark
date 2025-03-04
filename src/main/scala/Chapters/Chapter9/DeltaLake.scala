package Chapters.Chapter9

import Chapters.Chapter
import org.apache.spark.sql._
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._

object DeltaLake extends Chapter{

  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val sourcePath = args(0)
    val deltaPath = args(1)

    spark.sql("CREATE DATABASE learning_spark_db")
    spark.sql("USE learning_spark_db")

    spark.read
      .format("parquet")
      .load(sourcePath)
      .write
      .format("delta")
      .save(deltaPath)

    spark.read
      .format("delta")
      .load(deltaPath)
      .createOrReplaceTempView("loans_delta")

    spark.sql("SELECT * FROM loans_delta").show()

    import spark.implicits._

    // Se pueden mergear esquemas en caso de que sean diferentes
    val loanUpdates = Seq((1111111L, 1000, 1000.0, "TX", false), (2222222L, 2000, 0.0, "CA", true))
      .toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

    loanUpdates.write
      .format("delta")
      .mode("append")
      .option("mergeSchema", "true")
      .save(deltaPath)

    // Se pueden realizar operaciones SQL directamente

    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    deltaTable.update(
      col("addr_state") === "OR",
        Map("addr_state" -> lit("WA"))
    )

    spark.sql("SELECT * FROM loans_delta").show()

    deltaTable.delete("funded_amnt >= paid_amnt")

    spark.sql("SELECT * FROM loans_delta").show()

    deltaTable
      .alias("t")
      .merge(loanUpdates.alias("s"), "t.load_id = s.load_id")
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

    spark.sql("SELECT * FROM loans_delta").show()
  }
}
