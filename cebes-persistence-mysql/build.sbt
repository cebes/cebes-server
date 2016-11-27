name := "cebes-persistence-mysql"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  //"com.google.guava" % "guava" % Common.guavaVersion,
  "com.google.inject" % "guice" % Common.guiceVersion,
  "mysql" % "mysql-connector-java" % Common.mySqlConnectorVersion
)