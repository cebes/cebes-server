name := "cebes-pipeline-serving"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

mainClass in assembly := Some("io.cebes.serving.Main")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test"
)
