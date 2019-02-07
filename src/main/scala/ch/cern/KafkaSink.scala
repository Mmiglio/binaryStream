package ch.cern

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties

import scala.collection.mutable

/*
/**
  * Wrapper for a Kafka Producer
  * @param createProducer
  * @return a kafka producer lazily when invoked
  */
class KafkaSink(createProducer: () => KafkaProducer[String,String]) extends Serializable {

  import org.apache.kafka.clients.producer.ProducerRecord

  lazy val producer = createProducer()

  def send(topic: String, record: String): Unit = {
    producer.send(new ProducerRecord(topic, record))
  }
}

object KafkaSink {

  def apply(config:Properties): KafkaSink = {

    val f = () => {
      val producer = new KafkaProducer[String, String](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(f)
  }
}*/

object KafkaSink {

  import scala.collection.JavaConverters._

  private val Producers = mutable.Map[Map[String, Object], KafkaProducer[String, String]]()

  def getOrCreateProducer(config: Map[String, Object]): KafkaProducer[String, String] = {

    val defaultConfig = Map()
    /*
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
     */

    val finalConfig = defaultConfig ++ config
    //val finalConfig = KafkaClientProperties.getProducerProperties

    Producers.getOrElseUpdate(
      finalConfig, {
        val producer = new KafkaProducer[String, String](finalConfig.asJava)

        sys.addShutdownHook {
          producer.close()
        }
        producer
      })
  }

}
