package Chapters.Chapter7

import Chapters.SparkSessionTestWrapper
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import java.io.PrintStream

class CacheTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("should cache DataFrame and show performance improvement") {
        // Ejecutar el código bajo prueba

        val caching = Chapters.Chapter7.Caching
        caching.run(sparkSession, Array.empty)

        assert(caching.secondTime < caching.firstTime)
    }

    test("should correctly calculate square values") {
        import sparkSession.implicits._

        val df = sparkSession.range(5).toDF("id").withColumn("square", $"id" * $"id")
        val results = df.collect()

        assert(results(0).getLong(1) == 0)  // 0*0
        assert(results(1).getLong(1) == 1)  // 1*1
        assert(results(2).getLong(1) == 4)  // 2*2
    }
}
