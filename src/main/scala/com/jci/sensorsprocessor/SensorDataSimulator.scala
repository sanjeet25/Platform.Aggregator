package com.jci.sensorsprocessor

import com.jci.sensorsprocessor.util.JsonFormats.formats
import java.nio.file.{Files, Paths}
import java.sql.Timestamp
import java.util.concurrent.{Executors, ThreadLocalRandom}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.json4s.jackson.Serialization
import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.control.NonFatal

object SensorDataSimulator {

  case class CliConf(kafkaBroker: String = null, sensorsFile: String = null, samplesTopic: String = null, period: Duration = null)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[CliConf]("sensor-data-aggregation") {
      opt[String]("kafkaBrokers").text("Kafka brokers host:port<,host:port>.").required.action((x, c) => c.copy(kafkaBroker = x))
      opt[String]("sensorsFile").text("Path to sensors file (might be in HDFS or local file system").required.action((x, c) => c.copy(sensorsFile = x))
      opt[String]("samplesTopic").text("Kafka topic where samples arrive").required.action((x, c) => c.copy(samplesTopic = x))
      opt[Duration]("period").text("Publishing period").required.action((x, c) => c.copy(period = x))
    }
    val cliConf = parser.parse(args, CliConf()).getOrElse(sys.exit(-1))
    val jsonBytes = Files.readAllLines(Paths.get(cliConf.sensorsFile))
    val sensors = jsonBytes.asScala.map(Serialization.read[SensorPoint])

    val serializer = new StringSerializer()
    val producer = new KafkaProducer(Map[String, AnyRef](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG ->  cliConf.kafkaBroker).asJava,
                                     serializer, serializer)

    val scheduler = Executors.newSingleThreadScheduledExecutor()

    val sendSensorsData = new Runnable {
      override def run() = {
        println(Console.RED + "producing raw samples" + Console.RESET)
        val futures = sensors map { s =>
          val now = System.currentTimeMillis
          producer.send(new ProducerRecord(cliConf.samplesTopic, Serialization.write(
                SensorSample(s.timeseriesId, ThreadLocalRandom.current.nextDouble(0, 100), new Timestamp(now), "Raw",
                           "+00:00", now / 1000, now / 1000))))
        }

        try producer.flush()
        catch { case ex: InterruptedException => println("Got interrupted while flushing?") }

        for (f <- futures) {
          try f.get
          catch { case NonFatal(e) => println("failed sending " + e)}
        }
        println(Console.RED + "done" + Console.RESET)
      }
    }
    scheduler.scheduleAtFixedRate(sendSensorsData, 0, cliConf.period.toMillis, MILLISECONDS)
  }
}
