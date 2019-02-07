package ch.cern

object KafkaClientProperties {

  val brokers = "10.64.22.40:9092,10.64.22.41:9092,10.64.22.42:9092"

  /**
    * @return Consumer Properties
    */
  def getConsumerProperties: Map[String, Object] = {
    val consumerConfig = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.serializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      "group.id" -> "40"
    )
    consumerConfig
  }

  /**
    * @return Producer Properties
    */
  def getProducerProperties: Map[String, Object] = {
    val producerConfig = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )
    producerConfig
  }

}
