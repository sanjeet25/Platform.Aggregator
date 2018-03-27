package com.jci.sensorsprocessor
package util

import java.util.TimeZone
import org.json4s.{DefaultFormats, FieldSerializer}

object JsonFormats {

  val sensorDataFields = FieldSerializer[SensorSample](
    FieldSerializer.renameTo("value", "val") orElse FieldSerializer.renameTo("metric", "Metric"),
    FieldSerializer.renameFrom("val", "value") orElse FieldSerializer.renameFrom("Metric", "metric"))

  private val UTC = TimeZone.getTimeZone("UTC")
  private val defaultFormats = new DefaultFormats {
    override def dateFormatter = {
      val f = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
      f.setTimeZone(UTC)
      f
    }
  }

  implicit val formats = defaultFormats + sensorDataFields
}
