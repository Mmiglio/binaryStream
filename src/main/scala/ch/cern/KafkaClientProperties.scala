package ch.cern

import java.util.Properties

object KafkaClientProperties {

  val brokers = "10.64.22.40:9092,10.64.22.41:9092,10.64.22.42:9092"

  /**
    * @return Consumer Properties
    */
  def getConsumerProperties: Properties = {
    val config: Properties = new Properties
    config.put("bootstrap.servers", brokers)
    config.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    config.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    config.put("group.id", "40")
    config
  }

  /**
    * @return Producer Properties
    */
  def getProducerProperties: Properties = {
    val config: Properties = new Properties
    config.put("bootstrap.servers", brokers)
    config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    config
  }

}
