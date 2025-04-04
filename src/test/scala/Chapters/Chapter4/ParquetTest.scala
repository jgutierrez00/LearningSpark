package Chapters.Chapter4

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ParquetTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper with Matchers{

    private val testParquetPath = "src/test/resources/Chapters/Chapter4/test_flights.parquet"
    private val outputPath = "target/test-output/parquet-test"

    override def afterAll(): Unit = {

        if(sparkSession != null) {
            sparkSession.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
            sparkSession.stop()
        }
    }

    test("Parquet should create temporary view and save data correctly") {

        Parquet.run(sparkSession, Array(testParquetPath, outputPath))

        val tempViews = sparkSession.catalog.listTables().collect()

        tempViews.exists(_.name == "us_delay_flights_tbl") shouldBe (true)

        val outputDF = sparkSession.read.parquet(outputPath)
        outputDF.count() should be > 0L
    }
}
