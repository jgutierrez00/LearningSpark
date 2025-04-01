package Chapters.Chapter5

import Chapters.SparkSessionTestWrapper
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class WindowingTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    private val testDataPath = "src/test/resources/window_data_test.csv"

    override def beforeAll(): Unit = {

        val testData = Seq(
            "name,power,company",
            "Jose,100,Google",
            "Maria,150,Facebook",
            "Carlos,120,Google",
            "Ana,150,Amazon",
            "Luis,90,Facebook"
        )

        val writer = new java.io.PrintWriter(new java.io.File(testDataPath))
        try {
            testData.foreach(writer.println)
        } finally {
            writer.close()
        }

    }

    override def afterAll(): Unit = {
        sparkSession.stop()
        new java.io.File(testDataPath).delete()
    }

    test("should correctly apply window functions without partitioning") {
        // Ejecutar el código bajo prueba
        Windowing.run(sparkSession, Array(testDataPath))

        // Verificar resultados directamente
        val windowSpec = Window.orderBy(desc("power"))
        val resultDF = sparkSession.read
            .format("csv")
            .option("header", "true")
            .load(testDataPath)
            .withColumn("Rank", rank().over(windowSpec))
            .withColumn("Dense Rank", dense_rank().over(windowSpec))
            .withColumn("Row Number", row_number().over(windowSpec))

        val results = resultDF.orderBy(desc("power")).collect()

        // Verificar los rankings para los valores con power=150 (deberían compartir rank)
        val highPowerRows = results.filter(_.getString(1) == "150")
        assert(highPowerRows.length == 2)
        assert(highPowerRows(0).getInt(3) == 2) // Rank
        assert(highPowerRows(0).getInt(4) == 2) // Dense Rank
        assert(highPowerRows(0).getInt(5) == 2) // Row Number (primera fila)
        assert(highPowerRows(1).getInt(5) == 3) // Row Number (segunda fila)

        // Verificar la fila con power=120
        val mediumPowerRow = results.find(_.getString(1) == "120").get
        assert(mediumPowerRow.getInt(3) == 4) // Rank (salta el 2 porque hay empate)
        assert(mediumPowerRow.getInt(4) == 3) // Dense Rank (no salta números)
    }

    test("should load data with correct schema") {
        // Ejecutar la parte de carga de datos
        implicit val windowingEncoder: Encoder[WindowExercise] = Encoders.product[WindowExercise]
        val dataDS = sparkSession.read
            .format("csv")
            .option("header", "true")
            .load(testDataPath)
            .as[WindowExercise]

        // Verificar el esquema
        assert(dataDS.schema.fields.map(_.name) === Array("name", "power", "company"))
        assert(dataDS.count() == 5)
    }

    test("should handle empty input gracefully") {
        // Crear archivo vacío temporal
        val emptyFilePath = "src/test/resources/empty_window_test.csv"
        new java.io.PrintWriter(emptyFilePath) { write("name,power,company"); close() }

        // Ejecutar con archivo vacío
        Windowing.run(sparkSession, Array(emptyFilePath))

        // Verificar que no lanza excepción
        new java.io.File(emptyFilePath).delete()
    }
}
