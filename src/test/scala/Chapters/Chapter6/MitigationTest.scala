package Chapters.Chapter6

import org.scalatest.funsuite.AnyFunSuite
import Chapters.SparkSessionTestWrapper
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.year
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.functions._
import java.io.{PrintStream, PrintWriter}
import java.nio.file.{Files, Paths}

case class Person(firstName: String, lastName: String, birthDate: String, salary: Double)

class MitigationTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

    private val testJsonPath = "src/test/resources/Chapters/Chapter6/persons_test.json"

    override def beforeAll(): Unit = {
        val testData = """
                         |[
                         |  {"firstName": "David", "lastName": "Johnson", "birthDate": "1985-05-15", "salary": 90000},
                         |  {"firstName": "Daniel", "lastName": "Jackson", "birthDate": "1990-11-22", "salary": 85000},
                         |  {"firstName": "John", "lastName": "Smith", "birthDate": "1970-08-30", "salary": 75000},
                         |  {"firstName": "Diana", "lastName": "James", "birthDate": "1960-03-10", "salary": 95000}
                         |]
                         |""".stripMargin

        val writer = new PrintStream(testJsonPath)
        try{
            writer.println(testData)
        } finally {
            writer.close()
        }
    }

    override def afterAll(): Unit = {
        sparkSession.stop()
        Files.deleteIfExists(Paths.get(testJsonPath))
    }


    test("should filter persons correctly with lambda functions"){

        val outCapture = new java.io.ByteArrayOutputStream()
        Console.withOut(outCapture){
            Mitigation.run(sparkSession, Array(testJsonPath))
        }

        val output = outCapture.toString.split("\n")

        val lambdaCount = output(0).trim.toInt
        val columnCount = output(1).trim.toInt

        assert(lambdaCount == 1)
        assert(columnCount == 1)

    }

//    test("should handle empty input gracefully"){
//
//        val emptyPath = "src/test/resources/Chapters/Chapter6/empty_test.json"
//        new PrintWriter(emptyPath){ write("[]"); close() }
//
//        Mitigation.run(sparkSession, Array(emptyPath))
//
//        Files.deleteIfExists(Paths.get(emptyPath))
//    }

    test("should correctly calculate earliest year"){

        val currentYear = java.util.Calendar.getInstance.get(java.util.Calendar.YEAR)
        val earliestYear = currentYear - 40

        val personDF = sparkSession.read
            .format("json")
            .option("path", testJsonPath)
            .option("multiline", "true")
            .load
            .as[Person](Encoders.product[Person])

        val filteredCount = personDF
            .filter(year(col("birthDate")) > earliestYear)
            .filter(col("salary") > 80000)
            .filter(col("lastName").startsWith("J"))
            .filter(col("firstName").startsWith("D"))
            .count()

        assert(filteredCount == 1)
    }

}
