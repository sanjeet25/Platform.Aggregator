package com.jci.sensorsprocessor

import java.nio.file.{Paths}
import java.util.UUID
import org.json4s.jackson.Serialization

object SensorPointGenerator extends App {

  val customers = 1
  val sitesPerCustomer = 1
  val chillersPerSite = 2
  val pointsPerChiller = 1
//  val customers = 2
//  val sitesPerCustomer = 2
//  val chillersPerSite = 2
//  val pointsPerChiller = 25
  val timeseriesPerPoint = 1
  val totalSize = customers * sitesPerCustomer * chillersPerSite * pointsPerChiller * timeseriesPerPoint

  val sensors = for {
    customer <- (0 until customers).iterator
    customerId = UUID.randomUUID.toString
    site <- (0 until sitesPerCustomer).iterator
    siteId = UUID.randomUUID.toString
    chiller <- (0 until chillersPerSite).iterator
    chillerId = UUID.randomUUID.toString
    point <- (0 until pointsPerChiller).iterator
    timeseries <- (0 until timeseriesPerPoint).iterator
  } yield {
    SensorPoint(
      pointId = UUID.randomUUID.toString,
      pointType = "mot-sts",
      pointName = "Motor Status",
      timeseriesId = UUID.randomUUID.toString,
      chillerId = chillerId,
      chillerName = s"Chiller $chillerId",
      siteId = siteId,
      siteName = "507 Michigan St Milwaukee",
      customerId = customerId,
      customerName = "Jonshon Controls"
    )
  }

  println("writing")
  val out = new java.io.PrintStream(Paths.get("target/sensors.json").toFile)
//  out.println("[")
//  sensors.zipWithIndex.foreach { case (e, idx) =>
//    out.print(Serialization.writePretty(e)(util.JsonFormats.formats))
//    if (idx != (totalSize - 1)) out.println(",")
//    else out.println()
//  }
//  out.println("]")
  sensors.foreach(e => out.println(Serialization.write(e)(util.JsonFormats.formats)))
  out.close()
  println("done")
//  Files.write(Paths.get("target/sensors.json"), json.getBytes("utf-8"))
}
