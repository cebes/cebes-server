name := "cebes-spark"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.amazonaws" % "aws-java-sdk-s3" % Common.awsJavaSdkS3,
  "com.google.inject" % "guice" % Common.guiceVersion,

  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided"
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-mllib" % Common.sparkVersion % "provided"
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % Common.sparkVersion % "provided"
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-hive" % Common.sparkVersion % "provided",

  // accelerated maths for spark-ml
  "com.github.fommil.netlib" % "all" % Common.netlibVersion,

  // fix problems with hadoop-client dependency in spark-core
  // https://stackoverflow.com/questions/36427291/illegalaccesserror-to-guavas-stopwatch-from-org-apache-hadoop-mapreduce-lib-inp
  "org.apache.hadoop" % "hadoop-client" % Common.hadoopClientVersion
    exclude("com.google.inject", "guice")
    exclude("org.slf4j", "slf4j-log4j12"),

  // support S3 in hadoop, using the same version with hadoop-client
  "org.apache.hadoop" % "hadoop-aws" % Common.hadoopClientVersion
    exclude("com.amazonaws", "aws-java-sdk")
)
