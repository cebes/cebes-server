name := "cebes-dataframe-spark"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % Common.sparkVersion % "provided"
    exclude("com.google.inject", "guice")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-hive" % Common.sparkVersion % "provided",

  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.22",

  "com.google.inject" % "guice" % Common.guiceVersion,

  "mysql" % "mysql-connector-java" % Common.mySqlConnectorVersion
)
