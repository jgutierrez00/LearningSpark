package Chapters.Chapter2

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class AuthorsAgesTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("calculate average ages correctly") {
        // Ejecutamos el método bajo prueba
        AuthorsAges.run(sparkSession, Array.empty)

        // Podemos verificar los resultados de varias formas:
        // Opción 1: Capturando el output (para pruebas simples)
        // Opción 2: Extrayendo la lógica a un método testable (mejor práctica)

        // Implementación de la opción 2:
        val testData = Seq(("Brooke", 20), ("Brooke", 25), ("Denny", 31), ("Jules", 30), ("TD", 35))
        val expectedAverages = Map(
            "Brooke" -> 22.5,
            "Denny" -> 31.0,
            "Jules" -> 30.0,
            "TD" -> 35.0
        )

        val dataDF = sparkSession.createDataFrame(testData).toDF("name", "age")
        val resultDF = dataDF.groupBy("name").agg(org.apache.spark.sql.functions.avg("age"))

        // Verificamos los resultados
        val results = resultDF.collect().map(row => (row.getString(0), row.getDouble(1))).toMap
        assert(results == expectedAverages)
    }

    test("handle empty input gracefully") {
        // Probamos con datos vacíos
        val emptyDF = sparkSession.createDataFrame(Seq.empty[(String, Int)]).toDF("name", "age")
        val resultDF = emptyDF.groupBy("name").agg(org.apache.spark.sql.functions.avg("age"))

        assert(resultDF.count() == 0)
    }
}
