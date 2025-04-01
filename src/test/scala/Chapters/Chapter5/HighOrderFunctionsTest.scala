package Chapters.Chapter5

import Chapters.SparkSessionTestWrapper
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll


class HighOrderFunctionsTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("should correctly transform celsius to fahrenheit") {
        // Arrange
        val t1 = Array(35, 36, 32, 30, 40, 42, 38)
        val t2 = Array(31, 32, 34, 55, 56)
        val tempDF = sparkSession.createDataFrame(Seq(
            (1, t1),
            (2, t2)
        )).toDF("id", "celsius")

        tempDF.createOrReplaceTempView("tempDF")

        // Act
        val resultDF = sparkSession.sql(
            """SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit
              |FROM tempDF""".stripMargin)

        // Assert
        val results = resultDF.collect()
        assert(results.length == 2)

        // Verificar la conversión para el primer conjunto de temperaturas
        val fahrenheit1 = results(0).getAs[Seq[Int]]("fahrenheit")
        assert(fahrenheit1 === Seq(95, 96, 89, 86, 104, 107, 100))

        // Verificar la conversión para el segundo conjunto
        val fahrenheit2 = results(1).getAs[Seq[Int]]("fahrenheit")
        assert(fahrenheit2 === Seq(87, 89, 93, 131, 132))
    }

    test("should correctly filter temperatures above threshold") {
        // Arrange
        val t1 = Array(35, 36, 32, 30, 40, 42, 38)
        val tempDF = sparkSession.createDataFrame(Seq((1, t1))).toDF("id", "celsius")
        tempDF.createOrReplaceTempView("tempDF")

        // Act
        val resultDF = sparkSession.sql(
            """SELECT celsius, filter(celsius, t -> t > 38) as high
              |FROM tempDF""".stripMargin)

        // Assert
        val result = resultDF.collect()(0)
        assert(result.getAs[Seq[Int]]("high") === Seq(40, 42))
    }

    test("should correctly check if threshold temperature exists") {
        // Arrange
        val t1 = Array(35, 36, 32, 30, 40, 42, 38)
        val t2 = Array(31, 32, 34, 55, 56)
        val tempDF = sparkSession.createDataFrame(Seq(
            (1, t1),
            (2, t2)
        )).toDF("id", "celsius")

        tempDF.createOrReplaceTempView("tempDF")

        // Act
        val resultDF = sparkSession.sql(
            """SELECT celsius, exists(celsius, t -> t = 38) as threshold
              |FROM tempDF""".stripMargin)

        // Assert
        val results = resultDF.collect()
        assert(results(0).getAs[Boolean]("threshold") === true)  // Contiene 38
        assert(results(1).getAs[Boolean]("threshold") === false) // No contiene 38
    }

    test("should create DataFrame with correct schema") {
        // Arrange & Act
        val t1 = Array(35, 36, 32, 30, 40, 42, 38)
        val tempDF = sparkSession.createDataFrame(Seq((1, t1))).toDF("id", "celsius")

        val expectedSchema = StructType(Seq(
            StructField("id", IntegerType, false),
            StructField("celsius", ArrayType(IntegerType, false), true)
        ))

        assert(tempDF.schema === expectedSchema)
    }
}
