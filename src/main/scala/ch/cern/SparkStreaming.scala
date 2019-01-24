package ch.cern

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkStreaming {

  def main(args: Array[String]): Unit = {

    // Get the topic name
    if (args.length < 2){
      println("Input should be in the form <topic name> <batch-time (ms)>")
      System.exit(1)
    }
    val topic = Array(args(0))
    val brokers = "10.64.22.40:9092,10.64.22.41:9092,10.64.22.42:9092"
    val batchTime = args(1).toInt

    // Create streaming context
    val conf = new SparkConf().setAppName("DetectorStream")
    val ssc = new StreamingContext(conf, Milliseconds(batchTime)) //Milliseconds

    // remove log infos
    ssc.sparkContext.setLogLevel("ERROR")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[ByteArrayDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "40"
    )

    val producerConfig = Map[String, Object](
      "bootstrap.servers" -> brokers
    )

    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[Array[Byte], Array[Byte]](topic, kafkaParams)
    ).map(x => x.value())

    // similar to UInt32_t
    def asUInt(longValue: Long) = {
      longValue & 0x00000000FFFFFFFFL
    }

    val unpack = udf((record: Array[Byte]) => {

      val bb = ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN)
      val buffer = bb.getLong // get 8 bytes

      val HEAD        = asUInt((buffer >> 62) & 0x3)
      val FPGA        = asUInt((buffer >> 58) & 0xF)
      val TDC_CHANNEL = asUInt((buffer >> 49) & 0x1FF) + 1
      val ORBIT_CNT   = asUInt((buffer >> 17) & 0xFFFFFFFF)
      val BX_COUNTER  = asUInt((buffer >> 5) & 0xFFF)
      val TDC_MEANS   = asUInt((buffer >> 0) & 0x1F) - 1

      Array(HEAD, FPGA, TDC_CHANNEL, ORBIT_CNT, BX_COUNTER, TDC_MEANS)
    })

    stream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){

        // Get the singleton instance of SparkSession
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._

        val df = rdd.toDF("batch")

        val convertedDF = df
          .withColumn("converted", unpack(df("batch")))
          .select(
            $"converted"(0).as("HEAD"),
            $"converted"(1).as("FPGA"),
            $"converted"(2).as("TDC_CHANNEL"),
            $"converted"(3).as("ORBIT_CNT"),
            $"converted"(4).as("BX_COUNTER"),
            $"converted"(5).as("TDC_MEANS")
          )

        val occupancy = convertedDF.groupBy("TDC_CHANNEL").count()

        // Collapse dataframe
        occupancy.createOrReplaceTempView("histogram")
        val histo2kafka = spark.sql(
          """
            |SELECT collect_list(TDC_CHANNEL) as TDC_CHANNEL, collect_list(count) as COUNT
            |FROM histogram
          """.stripMargin).toJSON

        histo2kafka.rdd.foreachPartition(partition => {
          val producer = KafkaProducerFactory.getOrCreateProducer(producerConfig)
          partition.foreach(record => {
            producer.send(new ProducerRecord[String, String]("plot",  record.toString))
            producer.flush()
          })
        })

      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
