package Chapters.Chapter7

import Chapters.SparkSessionTestWrapper
import org.apache.spark.storage.StorageLevel
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class PersistingTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("should persist DataFrame with DISK_ONLY storage level") {

        Persisting.run(sparkSession, Array.empty)

        val persistedRDDs = sparkSession.sparkContext.getPersistentRDDs

        assert(persistedRDDs.nonEmpty, "DataFrame should be persisted")
        assert(persistedRDDs.values.head.getStorageLevel == StorageLevel.DISK_ONLY)
    }

    test("should maintain data integrity after persisting"){

        import sparkSession.implicits._

        val df = sparkSession.range(5).toDF("id").withColumn("square", $"id" * $"id")
        df.persist(StorageLevel.MEMORY_AND_DISK)

        val countBefore = df.count()
        val countAfter = df.count()

        assert(countBefore == countAfter)
        assert(countBefore == 5)
    }

}
