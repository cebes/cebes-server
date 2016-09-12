name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.10",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.4.10",
  "com.softwaremill.akka-http-session" %% "core" % "0.2.7",

  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.7",

  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),

  "com.google.inject" % "guice" % "4.1.0"
)

mainClass in assembly := Some("io.cebes.server.Main")

dependencyOverrides += "org.scalatest" %% "scalatest_2.11" % "3.0.0"
