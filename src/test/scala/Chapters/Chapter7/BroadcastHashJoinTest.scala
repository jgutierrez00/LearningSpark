package Chapters.Chapter7

import Chapters.SparkSessionTestWrapper
import org.apache.spark.sql.functions.{broadcast, col}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class BroadcastHashJoinTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    private val personDataPath = "src/test/resources/person_data_test.json"
    private val countryDataPath = "src/test/resources/country_data_test.json"

    override def beforeAll(): Unit = {

        val personData = """
                           |[
                           |  {"name": "John", "country": "USA"},
                           |  {"name": "Maria", "country": "Spain"},
                           |  {"name": "Chen", "country": "China"},
                           |  {"name": "Pierre", "country": "France"}
                           |]
                           |""".stripMargin

        val countryData = """
                            |[
                            |  {"name": "USA", "capital": "Washington"},
                            |  {"name": "Spain", "capital": "Madrid"},
                            |  {"name": "China", "capital": "Beijing"}
                            |]
                            |""".stripMargin

        val personWriter = new java.io.PrintWriter(new java.io.File(personDataPath))
        val countryWriter = new java.io.PrintWriter(new java.io.File(countryDataPath))

        try {
            personWriter.println(personData)
            countryWriter.println(countryData)
        } finally {
            personWriter.close()
            countryWriter.close()
        }
    }

    override def afterAll(): Unit = {
        sparkSession.stop()
        new java.io.File(personDataPath).delete()
        new java.io.File(countryDataPath).delete()
    }

    test("should perform broadcast hash join correctly") {
        // Ejecutar el código bajo prueba
        BroadcastHashJoin.run(sparkSession, Array(personDataPath, countryDataPath))

        // Verificar el plan de ejecución para confirmar que se usa broadcast
        val personDF = sparkSession.read.json(personDataPath).drop("_corrupt_record")
        val countryDF = sparkSession.read.json(countryDataPath).drop("_corrupt_record").withColumnRenamed("name", "country_name")
        val joinExpr = personDF.col("country") === countryDF.col("country_name")

        val joinResult = personDF.join(broadcast(countryDF), joinExpr, "inner")
        val executionPlan = joinResult.queryExecution.executedPlan.toString

        assert(executionPlan.contains("BroadcastHashJoin"))
        assert(executionPlan.contains("BuildRight")) // El lado derecho (country) es el que se broadcast

        // Verificar los resultados del join
        val results = joinResult.collect()
        assert(results.length == 3) // Pierre (France) no debería aparecer

        // Verificar algunos valores específicos
        val usaEntry = joinResult.filter(col("name") === "John").select("capital").first()
        assert(usaEntry.getString(0) == "Washington")

        val chinaEntry = joinResult.filter(col("name") === "Chen").select("capital").first()
        assert(chinaEntry.getString(0) == "Beijing")
    }

    test("should handle empty datasets gracefully") {
        // Crear archivos vacíos temporales
        val emptyPersonPath = "src/test/resources/empty_person_test.json"
        val emptyCountryPath = "src/test/resources/empty_country_test.json"

        new java.io.PrintWriter(emptyPersonPath) { write("[]"); close() }
        new java.io.PrintWriter(emptyCountryPath) { write("[]"); close() }

        // Ejecutar y verificar que no lanza excepción
        BroadcastHashJoin.run(sparkSession, Array(emptyPersonPath, emptyCountryPath))

        // Limpiar
        new java.io.File(emptyPersonPath).delete()
        new java.io.File(emptyCountryPath).delete()
    }

    test("should drop corrupt records") {
        // Crear datos con registros corruptos
        val corruptPersonPath = "src/test/resources/corrupt_person_test.json"
        new java.io.PrintWriter(corruptPersonPath) {
            write("""{"name": "GoodRecord", "country": "USA"}\n{"invalid": "BadRecord"}""")
            close()
        }

        val personDF = sparkSession.read.json(corruptPersonPath)
        assert(!personDF.columns.contains("_corrupt_record"))

        val cleanDF = personDF.drop("_corrupt_record")
        assert(!cleanDF.columns.contains("_corrupt_record"))
        assert(cleanDF.count() == 1)

        new java.io.File(corruptPersonPath).delete()
    }

}
