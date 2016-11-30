name := "cebes-persistence-mysql"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % Common.guiceVersion,
  "mysql" % "mysql-connector-java" % Common.mySqlConnectorVersion
)