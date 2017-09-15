name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

mainClass in assembly := Some("io.cebes.server.Main")

// this is just to help IntelliJ determine the
// correct scalatest. It is not included in the assembly anyway.
dependencyOverrides += "org.scalatest" %% "scalatest" % Common.scalaTestVersion

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-testkit" % Common.akkaHttpVersion % "test",

  "org.apache.spark" %% "spark-core" % Common.sparkVersion % "provided"
    exclude("org.apache.hadoop", "hadoop-client")
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-mllib" % Common.sparkVersion % "provided"
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-sql" % Common.sparkVersion % "provided"
    exclude("org.scalatest", "scalatest_2.11"),
  "org.apache.spark" %% "spark-hive" % Common.sparkVersion % "provided"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("org.apache.commons.**" -> "shadedcommonsbeanutils.@1").
    inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
  ShadeRule.rename("org.apache.commons.**" -> "shadedcommonsbeanutilscore.@1").
    inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0")
)
