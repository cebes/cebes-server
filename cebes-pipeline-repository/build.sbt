name := "cebes-pipeline-repository"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

mainClass in assembly := Some("io.cebes.repository.Main")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-log4j12" % Common.log4j12Version,
  "org.squeryl" %% "squeryl" % Common.squerylVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test"
)
