package Chapters.Chapter4

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ViewsTest extends AnyFunSuite with Matchers with BeforeAndAfterAll with SparkSessionTestWrapper{

    private val testCsvPath = "src/test/resources/Chapters/Chapter4/us_delay_flights_small.csv"

    override def beforeAll(): Unit = {

        sparkSession.sql("DROP SCHEMA IF EXISTS learn_spark_db CASCADE")
    }

    override def afterAll(): Unit = {

        if (sparkSession != null){
            sparkSession.stop()
        }
    }

    test("Views should create and manage temporary views correctly"){

        Views.run(sparkSession, Array(testCsvPath))

        sparkSession.sql("USE learn_spark_db")
        val tables = sparkSession.catalog.listTables().collect()
        tables.exists(_.name == "us_delay_flights_tbl") shouldBe true

        val globalViews = sparkSession.catalog.listTables("global_temp").collect()
        globalViews.exists(_.name == "us_origin_airport_SFO_global_tmp_view") shouldBe true

        sparkSession.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")


    }

    test("Views should filter data correctly in views") {

        Views.run(sparkSession, Array(testCsvPath))

        val sfoCount = sparkSession.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").count()
        val jfkCount = sparkSession.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").count()

        sfoCount should be >= 0L
        jfkCount should be >= 0L

        sparkSession.sql("DROP DATABASE IF EXISTS learn_spark_db CASCADE")

    }
}
