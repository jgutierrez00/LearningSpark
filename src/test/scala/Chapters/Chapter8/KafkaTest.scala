package Chapters.Chapter8

import Chapters.SparkSessionTestWrapper
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerRecord }
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

class KafkaTest extends AnyFunSuite with BeforeAndAfterAll with SparkSessionTestWrapper{

    private var query: StreamingQuery = _
    private var kafkaBoostrapServer = "localhost:9092"
    private val topicName = "test-topic"

    override def afterAll(): Unit = {
        sparkSession.stop()
    }

    test("kafka should read messages correctly"){

        val producerProps = new Properties()
        producerProps.put("bootstrap.servers", kafkaBoostrapServer)

        val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](
            producerProps,
            new StringSerializer,
            new StringSerializer
        )

        producer.send(new ProducerRecord(topicName, "clave", "valor_prueba"))
        producer.close()

        val query = sparkSession.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaBoostrapServer)
            .option("subscribe", topicName)
            .option("startingOffsets", "earliest")
            .load()
            .writeStream
            .format("memory")
            .queryName("testQuery")
            .outputMode("append")
            .start()

        try {

            val startTime = System.currentTimeMillis()

            while (System.currentTimeMillis() - startTime < 30000 && sparkSession.sql("SELECT * FROM testQuery").isEmpty) {
                Thread.sleep(500)
            }

            val result = sparkSession.sql("SELECT * FROM testQuery")
            assert(result.count() == 5L)
        } finally {
            query.stop()
        }
    }

}
