name := "sensors-processor"
organization := "jci"
version := "0.1"
scalaVersion := "2.11.12"
fork := true
scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature", "-Yinfer-argument-types", "-Xlint", "-Yno-adapted-args", "-Ypartial-unification", "-Ywarn-inaccessible", "-Ywarn-infer-any", "-Ywarn-nullary-override", "-Ywarn-nullary-unit", "-Ywarn-numeric-widen")

lazy val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "com.github.scopt" %% "scopt" % "3.7.0",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  //"org.apache.kafka" % "kafka-streams" % "0.10.2.1",
  "org.json4s" %% "json4s-jackson" % "3.2.11",

  "org.slf4j" % "slf4j-api" % "1.7.25",
  "org.slf4j" % "slf4j-ext" % "1.7.25",
  //"org.slf4j" % "slf4j-log4j12" % "1.7.21" % "runtime",
  "org.apache.logging.log4j" % "log4j-api" % "2.10.0" % "runtime",
  "org.apache.logging.log4j" % "log4j-core" % "2.10.0" % "runtime",
  "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.10.0" % "runtime",

  "org.openjdk.jol" % "jol-core" % "0.9" % "test"
)

enablePlugins(JavaAppPackaging)
