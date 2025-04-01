package Chapters.Chapter1

import Chapters.SparkSessionTestWrapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class MnMcountTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("empty input should return empty dataframe") {
    import sparkSession.implicits._

        val emptyDF = Seq.empty[(String, String, Int)].toDF("State", "Color", "Count")
        val result = emptyDF.groupBy("State", "Color").count().collect()

        assert(result.isEmpty)
    }
}