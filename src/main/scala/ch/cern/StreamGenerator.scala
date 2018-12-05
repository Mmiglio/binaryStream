package ch.cern

import org.apache.spark.sql.SparkSession
import java.util

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}

object StreamGenerator {

  def main(args: Array[String]): Unit = {

    if (args.length < 2){
      println("Input should be in the form <topic name> <file-name>")
      return
    }

    val topic = args(0)
    val file = args(1)

    val spark= SparkSession.builder().appName("binaryStreamGenerator").getOrCreate()
    val sc = spark.sparkContext

    // Read the binary file ad split it into records
    // recordLength = 8 bytes, 64 bits
    val rdd = sc.binaryRecords(file, 8).cache()

    // Count() to trigger cache()
    val numRecords = rdd.count()
    println("Number of records: "+numRecords.toString)

    val startTimer = System.currentTimeMillis()
    rdd.foreachPartition(partition => {

      val props = new util.HashMap[String, Object]()

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dbnile-kafka-a-5:9093,dbnile-kafka-b-5:9093,dbnile-kafka-c-5:9093")

      //Authentication
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
      props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI")
      props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka")
      props.put(SaslConfigs.SASL_JAAS_CONFIG, "com.sun.security.auth.module.Krb5LoginModule required" +
        "useKeyTab=true " +
        "storeKey=true " +
        "keyTab=\"keytab_file\" " +
        "principal=\"migliori@CERN.CH\";")
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "mytruststore.jks")

      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

      val producer = new KafkaProducer[String, String](props)

      partition.foreach(record => {
        val msg = new ProducerRecord[String, String](topic, record.toString)
        producer.send(msg)
      })
    })

    val stopTimer = System.currentTimeMillis()
    sc.stop()

    println("Elapsed time: %.1f s".format((stopTimer-startTimer).toFloat/1000))
    println("Sent %d msg/s".format((numRecords.toFloat/((stopTimer-startTimer).toFloat/1000)).toInt))
  }

}
