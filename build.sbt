name := "cebes-server"

lazy val commonSettings = Seq(
  version := "0.1.0-SNAPSHOT",
  organization := "io.cebes",
  scalaVersion := "2.11.8",
  test in assembly := {},

  // generate test reports (likely for jenkins to pickup)
  (testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-report"),

  libraryDependencies ++= Seq(
    "com.typesafe.scala-logging" %% "scala-logging-slf4j" % Common.scalaLoggingSlf4jVersion,
    //"ch.qos.logback" % "logback-classic" % Common.logbackClassicVersion,

    "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
)

lazy val cebesProperties = project.in(file("cebes-properties")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*)
lazy val cebesAuth = project.in(file("cebes-auth")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*)
lazy val cebesPersistenceMysql = project.in(file("cebes-persistence-mysql")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesProperties)

lazy val cebesDataframe = project.in(file("cebes-dataframe")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*)
lazy val cebesDataframeSpark = project.in(file("cebes-dataframe-spark")).
  settings(commonSettings: _*).
  disablePlugins(AssemblyPlugin).
  dependsOn(cebesDataframe, cebesProperties)
lazy val cebesHttpServer = project.in(file("cebes-http-server")).
  settings(commonSettings: _*).
  dependsOn(cebesAuth, cebesPersistenceMysql, cebesDataframeSpark)

lazy val cebesServer = project.in(file(".")).
  settings(commonSettings: _*).
  aggregate(cebesProperties, cebesAuth, cebesPersistenceMysql,
    cebesDataframe, cebesDataframeSpark, cebesHttpServer)

// test in all other sub-projects, except cebesHttpServer
// http://stackoverflow.com/questions/9856204/sbt-skip-tests-in-subproject-unless-running-from-within-that-project
val testNoHttpServer = TaskKey[Unit]("testNoHttpServer")
testNoHttpServer <<= Seq(
  test in (cebesProperties, Test),
  test in (cebesAuth, Test),
  test in (cebesPersistenceMysql, Test),
  test in (cebesDataframe, Test),
  test in (cebesDataframeSpark, Test)
).dependOn

scalastyleConfig := baseDirectory.value / "build" / "scalastyle-config.xml"