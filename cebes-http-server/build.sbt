name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

mainClass in assembly := Some("io.cebes.server.Main")

// this is just to help IntelliJ determine the
// correct scalatest. It is not included in the assembly anyway.
dependencyOverrides += "org.scalatest" %% "scalatest" % Common.scalaTestVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test",

  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided"
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-mllib" % Common.sparkVersion % "provided"
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % Common.sparkVersion % "provided"
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-hive" % Common.sparkVersion % "provided"
)

assemblyShadeRules in assembly := Common.apacheCommonsShadeRules
