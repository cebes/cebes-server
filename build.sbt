name := "cebes-server"

lazy val commonSettings = Seq(
  version := Common.projectVersion,
  organization := Common.organizationName,
  scalaVersion := Common.scalaVersion,
  test in assembly := {}
)

lazy val cebesAuth = project.in(file("cebes-auth")).
  settings(commonSettings: _*)
lazy val cebesDataframe = project.in(file("cebes-dataframe")).
  settings(commonSettings: _*)
lazy val cebesDataFrameSpark = project.in(file("cebes-dataframe-spark")).
  settings(commonSettings: _*).
  dependsOn(cebesDataframe)
lazy val cebesHttpServer = project.in(file("cebes-http-server")).
  settings(commonSettings: _*).
  dependsOn(cebesAuth, cebesDataFrameSpark)

lazy val cebesServer = project.in(file(".")).
  settings(commonSettings: _*).
  aggregate(cebesAuth, cebesHttpServer, cebesDataframe, cebesDataFrameSpark)

scalastyleConfig := baseDirectory.value / "build" / "scalastyle-config.xml"