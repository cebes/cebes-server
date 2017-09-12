name := "cebes-http-common"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http" % Common.akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % Common.akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test",
  "com.softwaremill.akka-http-session" %% "core" % Common.akkaHttpSessionVersion,

  "com.google.inject" % "guice" % Common.guiceVersion
)
