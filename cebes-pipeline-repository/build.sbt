name := "cebes-pipeline-repository"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

mainClass in assembly := Some("io.cebes.repository.Main")

libraryDependencies ++= Seq(
  "org.squeryl" %% "squeryl" % Common.squerylVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test"
)
