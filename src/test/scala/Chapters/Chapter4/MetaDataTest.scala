package Chapters.Chapter4

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.{allOf, convertToAnyShouldWrapper}

class MetaDataTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    private val testCsvPath = "src/test/resources/Chapters/Chapter4/us_delay_flights_small.csv"

    override def afterAll(): Unit = {

        if (sparkSession != null) {

            sparkSession.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")
            sparkSession.stop()
        }
    }

    test("Metadata should create database and table correctly"){

        MetaData.run(sparkSession, Array(testCsvPath))

        val databases = sparkSession.catalog.listDatabases().collect()
        databases.exists(_.name == "learn_spark_db") shouldBe (true)

        val columns = sparkSession.catalog.listColumns("us_delay_flights_tbl").collect()
        columns.map(_.name) should contain allOf ("date", "delay", "distance", "origin", "destination")

    }

    test("Metadata should load data correctly into the table"){

        val rowCount = sparkSession.sql("SELECT * FROM learn_spark_db.us_delay_flights_tbl").count()

        val df = sparkSession.table("learn_spark_db.us_delay_flights_tbl")
        df.schema.fields.map(_.name) should contain allOf ("date", "delay", "distance", "origin", "destination")

        df.schema("date").dataType.typeName shouldBe "string"
        df.schema("delay").dataType.typeName shouldBe "string"
        df.schema("distance").dataType.typeName shouldBe "string"
    }
}
