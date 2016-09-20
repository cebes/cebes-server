name := "cebes-properties"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % Common.guiceVersion,
  "com.typesafe" % "config" % Common.typeSafeConfigVersion
)