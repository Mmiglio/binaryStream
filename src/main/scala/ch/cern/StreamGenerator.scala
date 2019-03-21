package ch.cern

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.sql.SparkSession

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object StreamGenerator {

  def main(args: Array[String]): Unit = {

    if (args.length < 4){
      println("Input should be in the form <topic name> <file-name> <delay ms> <iterations>")
      System.exit(1)
    }

    val topic = args(0)
    val file = args(1)
    val delay_ms = args(2).toLong
    val iterations = args(3).toInt

    val spark= SparkSession.builder().appName("binaryStreamGenerator").getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // Read the binary file ad split it into records
    // recordLength = 8 bytes, 64 bits
    val rdd = sc.binaryRecords(file, 1024).cache()

    //repartition by the number of executor
    val numExecutors = sc.getConf.getInt("spark.executor.instances", 1)
    println("Number of executors: "+numExecutors)

    // Count() to trigger cache()
    println("Reading files ...")
    val numRecords = rdd.repartition(numExecutors).count()
    println("Number of records: "+numRecords.toString)
    println("Number of iterations: "+iterations)


    println("Streaming ...")
    val startTimer = System.currentTimeMillis()

    for(it <- 1 to iterations) {
      println("Iteration: " + it)
      val future = rdd.foreachPartitionAsync(partition => {

        val props = new util.HashMap[String, Object]()

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.64.22.40:9092,10.64.22.41:9092,10.64.22.42:9092")

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")

        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000")

        val producer = new KafkaProducer[Array[Byte], Array[Byte]](props)
        sys.addShutdownHook {
          producer.close()
        }

        partition.foreach(record => {
          val msg = new ProducerRecord[Array[Byte], Array[Byte]](topic, record)
          producer.send(msg)
          if (delay_ms != 0) {
            Thread.sleep(delay_ms)
          }
        })

        producer.close()
      })

      val completion = Await.result(future, Duration.Inf)
      println(completion)
    }

    val stopTimer = System.currentTimeMillis()
    sc.stop()

    println("Elapsed time: %.1f s".format((stopTimer-startTimer).toFloat/1000))
    println("Sent %d msg/s".format((numRecords.toFloat/((stopTimer-startTimer).toFloat/1000)).toInt))
  }

}


