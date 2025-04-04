package Chapters.Chapter6

import Chapters.SparkSessionTestWrapper
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.functions.desc
import org.scalatest.{BeforeAndAfterAll, ScalaTestVersion}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.Random

case class Usage(uid: Int, uname: String, usage: Int)

class SampleDataTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    implicit val usageEnconder: Encoder[Usage] = Encoders.product[Usage]


    override def afterAll(): Unit = {
        sparkSession.close()
    }

    test("should create Dataset with random Usage data"){

        SampleData.run(sparkSession, Array.empty)

        val r = new Random(42)
        val data = for (i <- 0 to 1000)
            yield Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))

        val dsUsage = sparkSession.createDataset(data).as[Usage](Encoders.product[Usage])

        assert(dsUsage.count() == 1001)
        assert(dsUsage.columns.sameElements(Array("uid", "uname", "usage")))
    }

    test("should filter usage greater than 900"){

        val r = new Random(42)
        val data = for(i <- 0 to 1000)
            yield Usage(i, "user-" + r.alphanumeric.take(5).mkString(""), r.nextInt(1000))

        val dsUsage = sparkSession.createDataset(data).as[Usage](Encoders.product[Usage])
        val filtered = dsUsage.filter(d => d.usage > 900).orderBy(desc("usage"))
        assert(filtered.count() < dsUsage.count())
        assert(filtered.columns.sameElements(Array("uid", "uname", "usage")))
    }

    test("should compute cost usage correctly"){

        val testUsage = Usage(1, "test", 800)
        val testUsage2 = Usage(2, "test2", 700)

        assert(SampleData.computeCostUsage(testUsage.usage) == 800 * 0.15)
        assert(SampleData.computeCostUsage(testUsage2.usage) == 700 * 0.50)
    }


}
