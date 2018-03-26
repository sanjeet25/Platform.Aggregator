package com.jci.sensorsprocessor

import com.jci.sensorsprocessor.util.JsonFormats.formats
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, Dataset, functions => sqlFunctions}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.json4s.jackson.Serialization
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object SensorDataAggregationJob {

  val logger = LogManager.getLogger("com.jci.sensors")

  case class CliConf(kafkaBroker: String = null, sensorsFile: String = null, samplesTopic: String = null, aggregatesTopic: String = null,
                     timeWindows: Seq[Duration] = Seq(Minutes(60), Minutes(60 * 24), Minutes(60 * 24 * 7)), period: Duration = Minutes(15),
                     local: Boolean = false)

  def main(args: Array[String]): Unit = {
    implicit val durationRead = scopt.Read.doubleRead.map(d => Seconds((d * 60).toLong))
    val parser = new scopt.OptionParser[CliConf]("sensor-data-aggregation") {
      opt[String]("kafkaBrokers").text("Kafka brokers host:port<,host:port>.").required.action((x, c) => c.copy(kafkaBroker = x))
      opt[String]("sensorsFile").text("Path to sensors file (might be in HDFS or local file system").required.action((x, c) => c.copy(sensorsFile = x))
      opt[String]("samplesTopic").text("Kafka topic where samples arrive").required.action((x, c) => c.copy(samplesTopic = x))
      opt[String]("aggregatesTopic").text("Kafka topic where output is published").required.action((x, c) => c.copy(aggregatesTopic = x))
      opt[Seq[Duration]]("timeWindows").text("Comma separated list of durations (in minutes) different time windows").action(
        (x, c) => c.copy(timeWindows = x.sortBy(_.milliseconds)))
      opt[Duration]('p', "period").action((x, c) => c.copy(period = x))
      opt[Unit]('l', "local").text("set the spark master to local[*]").required.action((x, c) => c.copy(local = true))
    }
    val cliConf = parser.parse(args, CliConf()).getOrElse(sys.exit(-1))

    val conf = new SparkConf().setAppName("sensor-data-aggregation")
    if (cliConf.local) conf.setMaster("local[*]")
    val ssc = new StreamingContext(conf, cliConf.period)


    val sensors = ssc.sparkContext.textFile(cliConf.sensorsFile).map(Serialization.read[SensorPoint]).collect()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    val sensorsDataFrame = spark.createDataset(sensors)

    val kafkaParams = Map[String, AnyRef](
      "bootstrap.servers" -> cliConf.kafkaBroker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> java.lang.Boolean.FALSE
    )

    val topics = Seq(cliConf.samplesTopic)
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams)).
      map(record => Serialization.read[SensorSample](record.value))

    cliConf.timeWindows foreach (w =>
      stream.window(w).foreachRDD(computeAndPublishAverage(ssc, sensorsDataFrame, cliConf.kafkaBroker, cliConf.aggregatesTopic, w) _))

    ssc.start()
    ssc.awaitTermination()
  }

  def computeAndPublishAverage(ssc: StreamingContext, sensorsDf: Dataset[SensorPoint], kafkaBroker: String,
                               aggregatesTopic: String, windowDuration: Duration)(rdd: RDD[SensorSample], batchTime: Time): Unit = {
    val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    implicit val sqlContext = spark.sqlContext
    import sqlContext.implicits._
    val sensorsDataDf = spark.createDataset(rdd)

    val resultDataset = sensorsDataDf.
    groupBy("timeseriesId").agg(
      sqlFunctions.count("value") as "valuesCount",
      sqlFunctions.min("value") as "valuesMin",
      sqlFunctions.max("value") as "valuesMax",
      sqlFunctions.avg("value") as "valuesAvg",
      sqlFunctions.sum("value") as "valuesSum",
      sqlFunctions.stddev("value") as "valuesStddev").
    join(sensorsDf, sensorsDataDf("timeseriesId") === sensorsDf("timeseriesId")).
    select(sensorsDf("*"), sqlFunctions.col("valuesCount"), sqlFunctions.col("valuesMin"), sqlFunctions.col("valuesMax"),
           sqlFunctions.col("valuesAvg"), sqlFunctions.col("valuesSum"), sqlFunctions.col("valuesStddev")).
    as[(String, String, String, String, String, String, String, String, String, String, Long, Double, Double, Double, Double, Double)]

    resultDataset.foreachPartition { iterator =>
      val serializer = new StringSerializer()
      val producer = new KafkaProducer(Map[String, AnyRef](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG ->  kafkaBroker).asJava,
                                       serializer, serializer)

      val timestampAggr = new java.sql.Timestamp(batchTime.milliseconds)
      val windowStart = new java.sql.Timestamp((batchTime - windowDuration).milliseconds)

      val allPosts = iterator map {
        case t@(pointId, pointType, pointName, timeseriesId, chillerId, chillerName, siteId, siteName, customerId, customerName,
                countOfSamples, miVal, maxVal, avgVal, sumVal, stdDev) =>
          producer.send(new ProducerRecord(aggregatesTopic, Serialization.write(
                TimeseriesAggregate(pointId, pointType, pointName, timeseriesId, chillerId, chillerName, siteId, siteName, customerId, customerName,
                                    timestampAggr, windowStart, timestampAggr, countOfSamples, miVal, maxVal,
                                    avgVal, sumVal, stdDev))))
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
