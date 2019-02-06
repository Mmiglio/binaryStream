package ch.cern

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.Properties

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
}
