name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % Common.akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % Common.akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test",
  "com.softwaremill.akka-http-session" %% "core" % "0.3.0",

  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-mllib" % Common.sparkVersion % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % Common.sparkVersion % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-hive" % Common.sparkVersion % "provided",

  "com.google.inject" % "guice" % Common.guiceVersion
)

mainClass in assembly := Some("io.cebes.server.Main")

// this is just to help IntelliJ determine the
// correct scalatest. It is not included in the assembly anyway.
dependencyOverrides += "org.scalatest" %% "scalatest" % "3.0.0"
