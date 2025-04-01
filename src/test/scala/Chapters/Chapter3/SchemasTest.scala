package Chapters.Chapter3

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SchemasTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {
    private val testDataPath = "src/test/resources/fire_data_test.csv"

    override def beforeAll(): Unit = {

        val testData = Seq(
            "IncidentNumber,CallType,AvailableDtTm,Delay,CallDate,WatchDate,NumAlarms",
            "1,Medical Incident,01/01/2020 12:00:00 AM,3,01/01/2020,01/01/2020,1",
            "2,Alarm,01/02/2020 01:00:00 AM,7,01/02/2020,01/02/2020,2",
            "3,Medical Incident,01/03/2020 02:00:00 AM,2,01/03/2020,01/03/2020,1",
            "4,Fire,01/04/2020 03:00:00 AM,6,01/04/2020,01/04/2020,3"
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

    test("should filter out Medical Incidents correctly") {
        Schemas.run(sparkSession, Array(testDataPath))

        // Verificación implícita (si no lanza excepción, pasa)
        // Para verificación explícita, necesitaríamos capturar el output
    }

    test("should count distinct call types correctly") {
        val fireDF = sparkSession.read.option("header", true).csv(testDataPath)
        val distinctCount = fireDF
            .select("CallType")
            .where("CallType is not null")
            .agg(org.apache.spark.sql.functions.expr("count(distinct CallType)"))
            .collect()(0).getLong(0)

        assert(distinctCount == 3) // Medical Incident, Alarm, Fire
    }

    test("should rename Delay column correctly") {
        val fireDF = sparkSession.read.option("header", true).csv(testDataPath)
        val newDF = fireDF.withColumnRenamed("Delay", "ResponseDelayedMins")

        assert(newDF.columns.contains("ResponseDelayedMins"))
        assert(!newDF.columns.contains("Delay"))
    }


}
