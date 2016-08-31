name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.9",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.4.9",
  "com.softwaremill.akka-http-session" %% "core" % "0.2.7",
  "com.softwaremill.akka-http-session" %% "jwt"  % "0.2.7",

  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.7",

  "com.google.inject" % "guice" % "4.1.0",

  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided"
)

mainClass in assembly := Some("io.cebes.server.Main")
