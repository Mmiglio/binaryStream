package ch.cern

import org.apache.spark.sql.SparkSession
import java.util

import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}

object StreamGenerator {

  def main(args: Array[String]): Unit = {

    if (args.length < 3){
      println("Input should be in the form <topic name> <file-name> <delay ms>")
      System.exit(1)
    }

    val topic = args(0)
    val file = args(1)
    val delay = args(2).toLong

    val spark= SparkSession.builder().appName("binaryStreamGenerator").getOrCreate()
    val sc = spark.sparkContext

    // Read the binary file ad split it into records
    // recordLength = 8 bytes, 64 bits
    val rdd = sc.binaryRecords(file, 8).cache()

    //repartition by the number of executor
    val numExecutors = sc.getConf.getInt("spark.executor.instances", 1)

    // Count() to trigger cache()
    val numRecords = rdd.repartition(numExecutors).count()
    println("Number of records: "+numRecords.toString)

    println("Streaming ...")
    val startTimer = System.currentTimeMillis()
    rdd.foreachPartition(partition => {

      val props = new util.HashMap[String, Object]()

      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "dbnile-kafka-a-5:9093,dbnile-kafka-b-5:9093,dbnile-kafka-c-5:9093")

      //Authentication
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
      props.put(SaslConfigs.SASL_MECHANISM, "GSSAPI")
      props.put(SaslConfigs.SASL_KERBEROS_SERVICE_NAME, "kafka")
      props.put(SaslConfigs.SASL_JAAS_CONFIG, "com.sun.security.auth.module.Krb5LoginModule required " +
        "useKeyTab=true " +
        "storeKey=true " +
        "keyTab=\"/afs/cern.ch/work/m/migliori/private/keytab_file\" " +
        "principal=\"migliori@CERN.CH\";")
      props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "/afs/cern.ch/work/m/migliori/private/mytruststore.jks")

      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

      val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)

      partition.foreach(record => {
        val msg = new ProducerRecord[Array[Byte], Array[Byte]](topic, record)
        producer.send(msg)
        Thread.sleep(delay)
      })
    })

    val stopTimer = System.currentTimeMillis()
    sc.stop()

    println("Elapsed time: %.1f s".format((stopTimer-startTimer).toFloat/1000))
    println("Sent %d msg/s".format((numRecords.toFloat/((stopTimer-startTimer).toFloat/1000)).toInt))
  }

}
