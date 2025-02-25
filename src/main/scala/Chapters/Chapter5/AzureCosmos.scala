package Chapters.Chapter5

import Chapters.Chapter

import org.apache.spark.sql._
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config

object AzureCosmos extends Chapter{
  override def run(spark: SparkSession, args: Array[String]): Unit = {

    val query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
    val readConfig = Config(Map(
      "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
      "Masterkey" -> "[MASTER KEY]",
      "Database" -> "[DATABASE]",
      "PreferredRegions" -> "Central US; East US2;",
      "Collection" -> "[COLLECTION]",
      "SamplingRatio" -> "1.0",
      "query_custom" -> query
    ))

    val df = spark.read.cosmosDB(readConfig)
    df.count()

    val writeConfig = Config(Map(
      "Endpoint" -> "https://[ACCOUNT].documents.azure.com:443/",
      "Masterkey" -> "[MASTER KEY]",
      "Database" -> "[DATABASE]",
      "PreferredRegions" -> "Central US;East US2;",
      "Collection" -> "[COLLECTION]",
      "WritingBatchSize" -> "100"
    ))

    import org.apache.spark.sql.SaveMode
    df.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)
  }
}
