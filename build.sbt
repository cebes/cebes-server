name := "cebes-server"

lazy val commonSettings = Seq(
  version := "0.5.0",
  organization := "io.cebes",
  scalaVersion := "2.11.8",
  test in assembly := {},

  // generate test reports (likely for jenkins to pickup)
  (testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-report"),

  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging" % Common.scalaLoggingVersion,
    "org.slf4j" % "slf4j-log4j12" % Common.log4j12Version % "test",
    "org.scalatest" %% "scalatest" % Common.scalaTestVersion % "test"
  )
)

lazy val cebesProperties = project.in(file("cebes-properties")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*)
lazy val cebesAuth = project.in(file("cebes-auth")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*)
lazy val cebesDataframe = project.in(file("cebes-dataframe")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*)

lazy val cebesPersistenceMysql = project.in(file("cebes-persistence-mysql")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesProperties, cebesDataframe)

lazy val cebesPipeline = project.in(file("cebes-pipeline")).
  settings(commonSettings: _*).
  dependsOn(cebesDataframe, cebesProperties)

lazy val cebesSpark = project.in(file("cebes-spark")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesDataframe, cebesPersistenceMysql, cebesPipeline)

lazy val cebesHttpServer = project.in(file("cebes-http-server")).
  settings(commonSettings: _*).
  dependsOn(cebesAuth, cebesSpark)

lazy val cebesServer = project.in(file(".")).
  settings(commonSettings: _*).
  aggregate(cebesProperties, cebesAuth, cebesDataframe,
    cebesPersistenceMysql, cebesPipeline, cebesSpark, cebesHttpServer)

scalastyleConfig := baseDirectory.value / "build" / "scalastyle-config.xml"
