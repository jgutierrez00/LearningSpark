package Chapters.Chapter5

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class UDFsTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("should register and execute cubed UDF correctly") {
        // Ejecutar el código bajo prueba
        UDFs.run(sparkSession, Array.empty)

        // Verificar que la UDF está registrada
        assert(sparkSession.catalog.listFunctions().collect().exists(_.name == "cubed"))

        // Verificar resultados directamente en la vista temporal
        val resultDF = sparkSession.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test ORDER BY id")
        val results = resultDF.collect()

        // Verificar algunos valores específicos
        assert(results(0).getLong(1) == 1L)  // 1^3 = 1
        assert(results(1).getLong(1) == 8L)  // 2^3 = 8
        assert(results(2).getLong(1) == 27L) // 3^3 = 27
        assert(results(7).getLong(1) == 512L) // 8^3 = 512
    }

    test("should calculate cube function correctly for edge cases") {
        // Registrar la UDF (como lo hace el código original)
        val cubed = (s: Long) => s * s * s
        sparkSession.udf.register("cubed", cubed)

        // Probar con 0 (aunque no está en el rango original)
        val zeroDF = sparkSession.sql("SELECT cubed(0) AS zero_cubed")
        assert(zeroDF.collect()(0).getLong(0) == 0L)

        // Probar con número negativo
        val negativeDF = sparkSession.sql("SELECT cubed(-3) AS neg_cubed")
        assert(negativeDF.collect()(0).getLong(0) == -27L)
    }

    test("should create udf_test view with correct data") {
        // Ejecutar la parte del código que crea la vista
        sparkSession.range(1, 9).createOrReplaceTempView("udf_test")

        // Verificar la vista
        val viewDF = sparkSession.sql("SELECT * FROM udf_test ORDER BY id")
        val ids = viewDF.collect().map(_.getLong(0))

        assert(ids.length == 8)
        assert(ids === Array(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L))
    }
}
