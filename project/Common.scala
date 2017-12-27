import sbt._
import sbtassembly.AssemblyPlugin.autoImport.ShadeRule

object Common {
  val sparkVersion = "2.2.0"
  val guiceVersion = "4.1.0"
  val guavaVersion = "19.0"
  val mariaDbConnectorVersion = "2.0.3"
  val typeSafeConfigVersion = "1.3.0"
  val scalaLoggingVersion = "3.5.0"
  val scalaTestVersion = "3.0.0"
  val akkaHttpVersion = "10.0.10"
  val akkaHttpSessionVersion = "0.5.1"
  val log4j12Version = "1.7.16" // should be the same with log4j12 in spark-core
  val sprayJsonVersion = "1.3.2" // should be the same with spray-json used in akka-http
  val netlibVersion = "1.1.2"
  val awsJavaSdkS3 = "1.11.22"
  val hadoopClientVersion = "2.7.3"
  val squerylVersion = "0.9.9"

  // shade rules for Apache Commons, to resolve assembly conflicts with Spark
  val apacheCommonsShadeRules = Seq(
    ShadeRule.rename("org.apache.commons.**" -> "shadedcommonsbeanutils.@1").
      inLibrary(ModuleID("commons-beanutils", "commons-beanutils", "1.7.0")),
    ShadeRule.rename("org.apache.commons.**" -> "shadedcommonsbeanutilscore.@1").
      inLibrary(ModuleID("commons-beanutils", "commons-beanutils-core", "1.8.0"))
  )


  val sparkDependencies = Seq(
    "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided"
      exclude("org.apache.hadoop", "hadoop-client")
      exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.spark" %% "spark-mllib" % Common.sparkVersion % "provided"
      exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.spark" %% "spark-sql" % Common.sparkVersion % "provided"
      exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.spark" %% "spark-hive" % Common.sparkVersion % "provided"
  )
}