name := "cebes-server"

lazy val commonSettings = Seq(
  version := "0.11.0-SNAPSHOT",
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

lazy val cebesPersistenceJdbc = project.in(file("cebes-persistence-jdbc")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesProperties, cebesDataframe)

lazy val cebesPipeline = project.in(file("cebes-pipeline")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesDataframe, cebesProperties)

lazy val cebesHttpCommon = project.in(file("cebes-http-common")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesPersistenceJdbc, cebesAuth)

lazy val cebesPipelineRepository = project.in(file("cebes-pipeline-repository")).
  settings(commonSettings: _*).
  dependsOn(cebesPipeline, cebesHttpCommon % "compile->compile;test->test")

lazy val cebesPipelineRepositoryClient = project.in(file("cebes-pipeline-repository-client")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesPipeline, cebesHttpCommon)

lazy val cebesSpark = project.in(file("cebes-spark")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  dependsOn(cebesDataframe, cebesPersistenceJdbc, cebesPipeline, cebesPipelineRepositoryClient)

lazy val cebesHttpServer = project.in(file("cebes-http-server")).
  settings(commonSettings: _*).
  dependsOn(cebesSpark, cebesHttpCommon % "compile->compile;test->test")

lazy val cebesPipelineServing = project.in(file("cebes-pipeline-serving")).
  settings(commonSettings: _*).
  dependsOn(cebesSpark, cebesPipelineRepositoryClient, cebesHttpCommon % "compile->compile;test->test")

lazy val cebesServer = project.in(file(".")).
  disablePlugins(AssemblyPlugin).
  settings(commonSettings: _*).
  aggregate(cebesProperties, cebesAuth, cebesDataframe,
    cebesPersistenceJdbc, cebesPipeline, cebesSpark, cebesHttpServer,
    cebesPipelineRepository, cebesPipelineRepositoryClient, cebesPipelineServing)

scalastyleConfig := baseDirectory.value / "build" / "scalastyle-config.xml"
