package Chapters.Chapter6

import Chapters.SparkSessionTestWrapper
import org.apache.spark.sql.Encoders
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.PrintWriter
import java.nio.file.{Files, Paths}

class CaseClassesTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

    private val testJsonPath = "src/test/resources/Chapters/Chapter6/bloggers_test.json"

    override def beforeAll(): Unit = {

        val testData =
            """
              |[
              |  {"id": 1, "url": "john.com", "published": "2020-01-01", "hits": 100, "campaigns": ["camp1", "camp2"]},
              |  {"id": 2, "url": "jane.com", "published": "2020-02-01", "hits": 200, "campaigns": ["camp3"]}
              |]
              |""".stripMargin

        val writer = new java.io.PrintWriter(new java.io.File(testJsonPath))
        try {
            writer.println(testData)
        } finally {
            writer.close()
        }
    }

    override def afterAll(): Unit = {
        sparkSession.stop()
        Files.deleteIfExists(Paths.get(testJsonPath))
    }

    test("should load JSON data into Bloggers case class"){

        CaseClasses.run(sparkSession, Array(testJsonPath))

        val bloggersDS = sparkSession.read
            .format("json")
            .option("path", testJsonPath)
            .load()
            .as[Bloggers](Encoders.product[Bloggers])

        val bloggers = bloggersDS.collect()

        assert(bloggers.length == 2)
        assert(bloggers(0).url == "john.com")
        assert(bloggers(1).hits == 200)
        assert(bloggers(0).campaigns.length == 2)
    }

    test("should handle empty JSON array"){

        val emptyPath = "src/test/resources/empty_bloggers_test.json"
        new PrintWriter(emptyPath) { write("[]"); close()}

        CaseClasses.run(sparkSession, Array(emptyPath))

        Files.deleteIfExists(Paths.get(emptyPath))
    }

}
