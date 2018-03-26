package com.jci.sensorsprocessor
package util

import java.sql.Timestamp
import java.time.OffsetDateTime
import java.time.ZoneOffset
import org.json4s.JsonAST.JString
import org.json4s.{CustomSerializer, DefaultFormats, FieldSerializer}

object JsonFormats {

  val sensorDataFields = FieldSerializer[SensorSample](
    FieldSerializer.renameTo("value", "val") orElse FieldSerializer.renameTo("metric", "Metric"),
    FieldSerializer.renameFrom("val", "value") orElse FieldSerializer.renameFrom("Metric", "metric"))

  val zonedDateTimeSerializer = new CustomSerializer[Timestamp](implicit formats => ({
        case s: JString => Timestamp.from(OffsetDateTime.parse(s.s).toInstant)
      }, {
        case ts: Timestamp => JString(OffsetDateTime.ofInstant(ts.toInstant, ZoneOffset.UTC).toString)
      }))

  implicit val formats = DefaultFormats + sensorDataFields + zonedDateTimeSerializer
}
