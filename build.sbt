name := "cebes-server"

lazy val commonSettings = Seq(
  version := Common.projectVersion,
  organization := Common.organizationName,
  scalaVersion := Common.scalaVersion,
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

scalastyleConfig := baseDirectory.value / "build" / "scalastyle-config.xml"