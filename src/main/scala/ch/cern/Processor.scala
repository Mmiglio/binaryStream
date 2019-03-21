package ch.cern

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer

object Processor {

  /**
    * Unpack an RDD and convert it into a DF
    * @param spark
    * @param rdd
    * @param nwords
    * @return
    */

  def unpackRDD(spark: SparkSession, rdd: RDD[Array[Byte]], nwords: Int): DataFrame = {

    // similar to UInt32_t
    def asUInt(longValue: Long) = {
      longValue & 0x00000000FFFFFFFFL
    }

    val transformed = rdd.map(batch => {
      val bb = ByteBuffer.wrap(batch).order(ByteOrder.LITTLE_ENDIAN)
      val hits = ArrayBuffer[Array[Long]]()

      for (_ <- 1 to nwords){

        val hit = bb.getLong
        val HEAD = asUInt((hit >> 62) & 0x3)
        val FPGA = asUInt((hit >> 58) & 0xF)
        val TDC_CHANNEL = asUInt((hit >> 49) & 0x1FF) + 1
        val ORBIT_CNT = asUInt((hit >> 17) & 0xFFFFFFFF)
        val BX_COUNTER = asUInt((hit >> 5) & 0xFFF)
        val TDC_MEANS = asUInt((hit >> 0) & 0x1F) - 1

        hits.append(Array(HEAD, FPGA, TDC_CHANNEL, ORBIT_CNT, BX_COUNTER, TDC_MEANS))
      }

      hits
    })

    import spark.implicits._
    val convertedDataframe = transformed
      .toDF("records")
      .withColumn("records", explode($"records"))
      .select(
        col("records")(0).as("HEAD"),
        col("records")(1).as("FPGA"),
        col("records")(2).as("TDC_CHANNEL"),
        col("records")(3).as("ORBIT_CNT"),
        col("records")(4).as("BX_COUNTER"),
        col("records")(5).as("TDC_MEANS")
      )

    convertedDataframe
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

    val occupancy = df.groupBy("FPGA","TDC_CHANNEL").count()

    // Collapse dataframe
    occupancy.createOrReplaceTempView("histogram")
    val histogram = spark.sql(
      """
        |SELECT collect_list(FPGA) as FPGA,
        |       collect_list(TDC_CHANNEL) as TDC_CHANNEL,
        |       collect_list(count) as COUNT
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
