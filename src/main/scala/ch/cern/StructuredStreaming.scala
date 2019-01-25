package ch.cern

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf, explode}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}

object StructuredStreaming {

  def main(args: Array[String]): Unit = {

    if (args.length < 1){
      println("Input should be in the form <topic name>")
      System.exit(1)
    }

    val topic = args(0)

    val spark: SparkSession = SparkSession
      .builder
      .appName("StructuredStreaming")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("Error")

    val rdd = spark.sparkContext.binaryRecords("file:///afs/cern.ch/work/m/migliori/lemma/data_000000.dat", 8)
    val df = rdd.toDF()

    val inputDF = spark
      .readStream.format("kafka")
      .option("subscribe", topic)
      .option("kafka.bootstrap.servers",
        "dbnile-kafka-a-5.cern.ch:9093," +
          "dbnile-kafka-b-5.cern.ch:9093," +
          "dbnile-kafka-c-5.cern.ch:9093")
      .option("kafka.security.protocol", "SASL_SSL")
      .option("kafka.sasl.mechanism", "GSSAPI")
      .option("kafka.sasl.kerberos.service.name", "kafka")
      .option("kafka.sasl.jaas.config",
        "com.sun.security.auth.module.Krb5LoginModule required " +
          "useKeyTab=true storeKey=true " +
          "keyTab=\"/afs/cern.ch/work/m/migliori/private/keytab_file\" " +
          "principal=\"migliori@CERN.CH\";")
      .option("kafka.ssl.truststore.location", "/afs/cern.ch/work/m/migliori/private/ts.jks")
      .option("kafka.ssl.truststore.password","migliori")
      .load
      .select($"value".as("event"))

    val convertUDF = udf((record: Array[Byte]) => {

      val bb = ByteBuffer.wrap(record).order(ByteOrder.nativeOrder())
      val buffer = bb.getLong // get 8 bytes

      val HEAD        = ((buffer >> 62) & 0x3).toInt
      val FPGA        = ((buffer >> 58) & 0xF).toInt
      val TDC_CHANNEL = ((buffer >> 49) & 0x1FF).toInt
      val ORBIT_CNT   = ((buffer >> 17) & 0xFFFFFFFF).toInt
      val BX_COUNTER  = ((buffer >> 5) & 0xFFF).toInt
      val TDC_MEANS   = ((buffer >> 0) & 0x1F).toInt

      Array(HEAD, FPGA, TDC_CHANNEL, ORBIT_CNT, BX_COUNTER, TDC_MEANS)
    })

    val convertedDF = inputDF
      .withColumn("converted", convertUDF(inputDF("event")))
      .select(
        $"converted"(0).as("HEAD"),
        $"converted"(1).as("FPGA"),
        $"converted"(2).as("TDC_CHANNEL"),
        $"converted"(3).as("ORBIT_CNT"),
        $"converted"(4).as("BX_COUNTER"),
        $"converted"(5).as("TDC_MEANS")
      )

    val occupancyDF = convertedDF
      .where($"FPGA"===1)
      .groupBy("TDC_CHANNEL")
      .count()

    val query: StreamingQuery = convertedDF.writeStream
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(100))
      .format("console")
      .queryName("StreamingQuery")
      .start()

    query.awaitTermination()
  }

}
