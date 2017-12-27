name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

mainClass in assembly := Some("io.cebes.server.Main")

// this is just to help IntelliJ determine the
// correct scalatest. It is not included in the assembly anyway.
dependencyOverrides += "org.scalatest" %% "scalatest" % Common.scalaTestVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test"
) ++ Common.sparkDependencies

assemblyShadeRules in assembly := Common.apacheCommonsShadeRules
