package ch.cern

import org.apache.spark.sql.SparkSession

import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkConf

object StreamProcessor {

  def main(args: Array[String]): Unit = {

    // List of parameters
    var inputTopic = ""
    var occupancyTopic = ""
    var eventTopic = ""
    var batchTime = 500

    // Get parameters: kafka topics and batch time
    try {
      inputTopic = args(0)
      occupancyTopic = args(1)
      eventTopic = args(2)
      batchTime = args(3).toInt
    } catch {
      case e: Exception => {
        println("Wrong number of parameters")
        println("Input should be in the form <input topic> <occupancy topic> <event topic> <batch-time (ms)>")
        System.exit(1)
      }
    }

    // Create streaming context
    val conf = new SparkConf().setAppName("DAQStream")
    val ssc = new StreamingContext(conf, Milliseconds(batchTime))
    ssc.sparkContext.setLogLevel("ERROR")

    // Create direct stream
    val stream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[Array[Byte], Array[Byte]](
        Array(inputTopic),
        KafkaClientProperties.getConsumerProperties
      )
    ).map(x => x.value())

    stream.foreachRDD(rdd => {
      if(!rdd.isEmpty()){

        // Get the singleton instance of SparkSession
        val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val df = rdd.toDF("batch")

        // Unpack the binary records
        val convertedDF = Processor.unpackDataFrame(df).cache()

        // Compute the occupancy
        val occupancyDF = Processor.computeOccupancy(convertedDF, spark)

        // Write the occupancy to kafka
        Processor.sentToKafka(occupancyDF, occupancyTopic)

        // Get the selected ORBITS_CNT based on the trigger
        val selectedOrbits = convertedDF
          .where($"TDC_CHANNEL"===139)
          .select("ORBIT_CNT")

        if(!selectedOrbits.take(1).isEmpty) {
          val events = Processor.createEvents(convertedDF, selectedOrbits, spark)
          Processor.sentToKafka(events, eventTopic)
        }

        //Unpersist the cached dataframe
        convertedDF.unpersist()
      }
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
