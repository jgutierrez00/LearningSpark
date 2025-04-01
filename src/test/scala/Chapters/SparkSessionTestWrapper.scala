package Chapters

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper{
    lazy val sparkSession: SparkSession = {
        val sparkConfig = new SparkConf()
        sparkConfig.set("spark.broadcast.compress", "false")
        sparkConfig.set("spark.shuffle.compress", "false")
        sparkConfig.set("spark.shuffle.spill.compress", "false")
        sparkConfig.set("spark.master", "local[*]")

        SparkSession.builder()
            .config(sparkConfig)
            .master("local[*]")
            .appName("spark-test")
            .getOrCreate()
    }
}
