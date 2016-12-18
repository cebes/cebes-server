name := "cebes-dataframe-json"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "io.spray" %% "spray-json" % Common.sprayJsonVersion
)