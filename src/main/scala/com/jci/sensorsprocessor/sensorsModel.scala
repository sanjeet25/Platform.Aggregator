package com.jci.sensorsprocessor

case class SensorPoint(
  pointId: String,
  pointType: String,
  pointName: String,
  timeseriesId: String,
  chillerId: String,
  chillerName: String,
  siteId: String,
  siteName: String,
  customerId: String,
  customerName: String
)

case class SensorSample(
  id: String,
  timeseriesId: String,
  value: Double,
  timestamp: java.sql.Timestamp,
  metric: String,
  timeOffset: String,
  unixTimestamp: Long,
  ingestUnixTimestamp: Long
)
object SensorSample {
  def apply(timeseriesId: String,
            value: Double,
            timestamp: java.sql.Timestamp,
            metric: String,
            timeOffset: String,
            unixTimestamp: Long,
            ingestUnixTimestamp: Long) = new SensorSample(s"$timeseriesId:$metric:$unixTimestamp", timeseriesId, value, timestamp, metric, timeOffset, unixTimestamp, ingestUnixTimestamp)
}

case class TimeseriesAggregate(
  pointId: String,
  pointType: String,
  pointName: String,
  timeseriesId: String,
  chillerId: String,
  chillerName: String,
  siteId: String,
  siteName: String,
  customerId: String,
  customerName: String,
  timestampAggregate: java.sql.Timestamp,
  timestampStartOfWindow: java.sql.Timestamp,
  timestampEndOfWindow: java.sql.Timestamp,
  countOfSamples: Long,
  minValOfSamples: Double,
  maxValOfSamples: Double,
  avgValOfSamples: Double,
  sumValOfSamples: Double,
  stddevValOfSamples: Double
)
object TimeseriesAggregate {
  def apply(sensor: SensorPoint,
            timestampAggregate: java.sql.Timestamp,
            timestampStartOfWindow: java.sql.Timestamp,
            timestampEndOfWindow: java.sql.Timestamp,
            countOfSamples: Long,
            minValOfSamples: Double,
            maxValOfSamples: Double,
            avgValOfSamples: Double,
            sumValOfSamples: Double,
            stddevValOfSamples: Double) = new TimeseriesAggregate(sensor.pointId, sensor.pointName, sensor.pointName, sensor.timeseriesId, sensor.chillerId,
                                                                  sensor.chillerName, sensor.siteId, sensor.siteName, sensor.customerId, sensor.customerName,
                                                                  timestampAggregate, timestampStartOfWindow, timestampEndOfWindow, countOfSamples,
                                                                  minValOfSamples, maxValOfSamples, avgValOfSamples, sumValOfSamples, stddevValOfSamples)
}

case class TimeseriesAggregate2(
  pointId: String,
  pointType: String,
  pointName: String,
  timeseriesId: String,
  chillerId: String,
  chillerName: String,
  siteId: String,
  siteName: String,
  customerId: String,
  customerName: String,
  timestampAggregate: java.sql.Timestamp,
  timestampStartOfWindow: java.sql.Timestamp,
  timestampEndOfWindow: java.sql.Timestamp,
  countOfSamples: Long,
  minValOfSamples: Double,
  maxValOfSamples: Double,
  avgValOfSamples: Double,
  sumValOfSamples: Double,
  delta: Double
)
object TimeseriesAggregate2 {
  def apply(sensor: SensorPoint,
            timestampAggregate: java.sql.Timestamp,
            timestampStartOfWindow: java.sql.Timestamp,
            timestampEndOfWindow: java.sql.Timestamp,
            countOfSamples: Long,
            minValOfSamples: Double,
            maxValOfSamples: Double,
            avgValOfSamples: Double,
            sumValOfSamples: Double,
            delta: Double) = new TimeseriesAggregate(sensor.pointId, sensor.pointName, sensor.pointName, sensor.timeseriesId, sensor.chillerId,
                                                                  sensor.chillerName, sensor.siteId, sensor.siteName, sensor.customerId, sensor.customerName,
                                                                  timestampAggregate, timestampStartOfWindow, timestampEndOfWindow, countOfSamples,
                                                                  minValOfSamples, maxValOfSamples, avgValOfSamples, sumValOfSamples, delta)
}
