package ch.cern

import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable

/*
Kafka Producer Factory
The factory creates only single instance of the producer for any given producer configuration.
If the producer instance has been already created, the existing instance is returned and reused.
 */
object KafkaProducerFactory {

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
