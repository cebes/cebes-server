name := "cebes-persistence-mysql"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % Common.guiceVersion,
  "mysql" % "mysql-connector-java" % Common.mySqlConnectorVersion,
  "org.apache.commons" % "commons-dbcp2" % "2.1.1",

  //http://stackoverflow.com/questions/13162671/missing-dependency-class-javax-annotation-nullable
  "com.google.code.findbugs" % "jsr305" % "3.0.+" % "compile"
)