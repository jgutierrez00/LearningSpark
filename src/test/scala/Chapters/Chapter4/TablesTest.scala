package Chapters.Chapter4

import Chapters.SparkSessionTestWrapper
import org.apache.hadoop.shaded.org.apache.avro.generic.GenericData
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.collection
import scala.collection.{immutable, mutable}

class TablesTest extends AnyFunSuite with SparkSessionTestWrapper with BeforeAndAfterAll with Matchers{

    val testCsvPath = "src/test/resources/Chapters/Chapter4/us_delay_flights_small.csv"

    override def afterAll(): Unit = {

        if (sparkSession != null){
            sparkSession.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
            sparkSession.stop()
        }
    }


    test("Tables should create managed table correctly"){

        Tables.run(sparkSession, Array(testCsvPath))

        sparkSession.sql("USE learn_spark_db")
        val tables = sparkSession.catalog.listTables().collect()
        tables.exists(_.name == "managed_us_delay_flights_tbl") should be (true)

        val columns = sparkSession.catalog.listColumns("managed_us_delay_flights_tbl").collect()
        columns.map(_.name) should contain allOf ("date", "delay", "distance", "origin", "destination")

        val rowCount = sparkSession.sql("SELECT * FROM managed_us_delay_flights_tbl").count()
        rowCount should be > 0L
    }
}
