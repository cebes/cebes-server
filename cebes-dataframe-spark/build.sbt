name := "cebes-dataframe-spark"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.22"
)