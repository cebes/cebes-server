name := "cebes-pipeline"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % Common.guiceVersion,
  "io.spray" %% "spray-json" % Common.sprayJsonVersion
)
