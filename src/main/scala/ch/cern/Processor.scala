package ch.cern

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Processor {

  /**
    * Convert a DataFrame containing binary record
    *
    * @param df
    * @return Converted DatataFrame
    *
    */
  def unpackDataFrame(df: DataFrame): DataFrame = {

    // similar to UInt32_t
    def asUInt(longValue: Long) = {
      longValue & 0x00000000FFFFFFFFL
    }

    // UDF used to unpack the messages
    val unpack = udf((record: Array[Byte]) => {

      val bb = ByteBuffer.wrap(record).order(ByteOrder.LITTLE_ENDIAN)
      val buffer = bb.getLong // get 8 bytes

      val HEAD = asUInt((buffer >> 62) & 0x3)
      val FPGA = asUInt((buffer >> 58) & 0xF)
      val TDC_CHANNEL = asUInt((buffer >> 49) & 0x1FF) + 1
      val ORBIT_CNT = asUInt((buffer >> 17) & 0xFFFFFFFF)
      val BX_COUNTER = asUInt((buffer >> 5) & 0xFFF)
      val TDC_MEANS = asUInt((buffer >> 0) & 0x1F) - 1

      Array(HEAD, FPGA, TDC_CHANNEL, ORBIT_CNT, BX_COUNTER, TDC_MEANS)
    })

    // Apply the udf to the dataframe and extract the columns
    df.withColumn("converted", unpack(df("batch")))
      .select(
        col("converted")(0).as("HEAD"),
        col("converted")(1).as("FPGA"),
        col("converted")(2).as("TDC_CHANNEL"),
        col("converted")(3).as("ORBIT_CNT"),
        col("converted")(4).as("BX_COUNTER"),
        col("converted")(5).as("TDC_MEANS")
      )
  }

  /**
    * Compute the histogram of the TDC and return the dataframe
    * composed by only one row containing
    * json = {"TDC_CHANNEL":[1,2,3..], "COUNT":[21,67,8,...]}
    *
    * @param df
    * @param spark
    * @return DataFrame with histogram
    *
    */
  def computeOccupancy(df: DataFrame, spark: SparkSession): DataFrame = {

    val occupancy = df.groupBy("TDC_CHANNEL").count()

    // Collapse dataframe
    occupancy.createOrReplaceTempView("histogram")
    val histogram = spark.sql(
      """
        |SELECT collect_list(TDC_CHANNEL) as TDC_CHANNEL, collect_list(count) as COUNT
        |FROM histogram
      """.stripMargin)
    spark.catalog.uncacheTable("histogram")
    histogram
  }

  /**
    *
    * Create a dataframe starting from the trigger signal
    *
    * @param df
    * @param selectedOrbits
    * @param spark
    * @return DataFrame with events
    */
  def createEvents(df: DataFrame, selectedOrbits: DataFrame, spark: SparkSession): DataFrame = {

    val events = df
      .join(selectedOrbits, "ORBIT_CNT")

    events.createOrReplaceTempView("events")
    val eventsDF = spark.sql(
      """
        |SELECT collect_list(FPGA) as FPGA,
        |       collect_list(TDC_CHANNEL) as TDC_CHANNEL,
        |       collect_list(ORBIT_CNT) as ORBIT_CNT,
        |       collect_list(BX_COUNTER) as BX_COUNTER,
        |       collect_list(TDC_MEANS) as TDC_MEANS
        |FROM events
      """.stripMargin)
    spark.catalog.uncacheTable("events")
    eventsDF
  }

  /**
    * Write the dataframe to a kafka topic
    *
    * @param df
    * @param ks
    * @param topic
    */

  def sendToKafka(df: DataFrame, topic: String): Unit = {
    df.toJSON.foreachPartition(partition => {
      val producer = KafkaProducerFactory.getOrCreateProducer(
        KafkaClientProperties.getProducerProperties
      )
      partition.foreach(record => {
        producer.send(new ProducerRecord[String, String](topic, record.toString))
        producer.flush()
      })
    })
  }

}
