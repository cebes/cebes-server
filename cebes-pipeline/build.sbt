name := "cebes-pipeline"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

libraryDependencies ++= Seq(
  //"io.grpc" % "grpc-all" % "1.0.3",
  "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion,
  "io.spray" %% "spray-json" % Common.sprayJsonVersion
)