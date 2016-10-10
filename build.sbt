name := "cebes-server"

lazy val commonSettings = Seq(
  version := "0.1.0-SNAPSHOT",
  organization := "io.cebes",
  scalaVersion := "2.11.8",
  test in assembly := {},

  // generate test reports (likely for jenkins to pickup, once we have jenkins...)
  (testOptions in Test) += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-report"),

  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
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
lazy val cebesDataframeSpark = project.in(file("cebes-dataframe-spark")).
  settings(commonSettings: _*).
  disablePlugins(AssemblyPlugin).
  dependsOn(cebesDataframe, cebesProperties)
lazy val cebesHttpServer = project.in(file("cebes-http-server")).
  settings(commonSettings: _*).
  dependsOn(cebesAuth, cebesDataframeSpark)

lazy val cebesServer = project.in(file(".")).
  settings(commonSettings: _*).
  aggregate(cebesAuth, cebesHttpServer, cebesDataframe, cebesDataframeSpark)

// test in all other sub-projects, except cebesHttpServer
// http://stackoverflow.com/questions/9856204/sbt-skip-tests-in-subproject-unless-running-from-within-that-project

// TODO: cebesHttpServer and cebesDataframeSpark can't run test in the same command
// because both of them use the same Derby metastore for Hive.
// Figure out a way to do this properly
// http://www.cloudera.com/documentation/archive/cdh/4-x/4-2-1/CDH4-Installation-Guide/cdh4ig_topic_18_4.html

val testNoHttpServer = TaskKey[Unit]("testNoHttpServer")
testNoHttpServer <<= Seq(
  test in (cebesProperties, Test),
  test in (cebesAuth, Test),
  test in (cebesDataframe, Test),
  test in (cebesDataframeSpark, Test)
).dependOn

scalastyleConfig := baseDirectory.value / "build" / "scalastyle-config.xml"