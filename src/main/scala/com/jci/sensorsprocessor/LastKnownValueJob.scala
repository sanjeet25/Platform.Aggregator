package com.jci.sensorsprocessor

import com.jci.sensorsprocessor.util.JsonFormats.formats
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions => sqlFunctions}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Duration => SparkDuration}
import org.json4s.jackson.Serialization
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NonFatal

object LastKnownValueJob {
  val logger = LoggerFactory.getLogger("com.jci.sensors")

  case class CliConf(kafkaBroker: String = null, sensorsFile: String = null, samplesTopic: String = null, lastKnownValueTopic: String = null,
                     period: Duration = 15.minutes)
  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[CliConf]("last-known-value") {
      opt[String]("kafkaBrokers").text("Kafka brokers host:port<,host:port>.").required.action((x, c) => c.copy(kafkaBroker = x))
      opt[String]("samplesTopic").text("Kafka topic where samples arrive").required.action((x, c) => c.copy(samplesTopic = x))
      opt[String]("lastKnownValueTopic").text("Kafka topic where last known values are published to").required.action((x, c) => c.copy(lastKnownValueTopic = x))
      opt[scala.concurrent.duration.Duration]('p', "period").action((x, c) => c.copy(period = x))
    }
    val cliConf = parser.parse(args, CliConf()).getOrElse(sys.exit(-1))

    val conf = new SparkConf().setMaster("local[*]").setAppName("sensor-data-aggregation")
    val ssc = new StreamingContext(conf, SparkDuration.apply(cliConf.period.toMillis))

    val kafkaParams = Map[String, AnyRef](
      "bootstrap.servers" -> cliConf.kafkaBroker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> java.lang.Boolean.FALSE
    )

    val samples = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](
        Seq(cliConf.samplesTopic, cliConf.lastKnownValueTopic), kafkaParams)).map(record => Serialization.read[SensorSample](record.value))


    samples.foreachRDD { (rdd, time) =>
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._

      val samlpes = spark.createDataset(rdd)
      val latestValues = samlpes.groupBy("timeseriesId").agg(samlpes("*"), sqlFunctions.max("ingestUnixTimestamp")).
        as[(String, String, Double, java.sql.Timestamp, String, String, Long, Long, Long)]


      val kafkaBroker = cliConf.kafkaBroker
      val samplesTopic = cliConf.lastKnownValueTopic
      latestValues.foreachPartition { rows =>
        val serializer = new StringSerializer()
        val producer = new KafkaProducer(Map[String, AnyRef](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG ->  kafkaBroker).asJava,
                                         serializer, serializer)

        val allPosts = rows map { tuple =>
            producer.send(new ProducerRecord(samplesTopic, Serialization.write(
                  SensorSample(tuple._1, tuple._2, tuple._3, tuple._4, tuple._5, tuple._6, tuple._7, tuple._8))))
        }

        try producer.flush()
        catch { case ex: InterruptedException => logger.error("got interrupted while waiting for the producer to finish sending data") }
        //after flushing we are guarnateed that the futures have completed, so check their results
        for (result <- allPosts) {
          try result.get
          catch { case NonFatal(ex) => logger.warn("Sensor publishing failed", ex) }
        }
        producer.close() //since our work uses microbatching, this means this executor is done publishing and we dispose the consumer.
      }
    }
  }
}
