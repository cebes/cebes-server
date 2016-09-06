name := "cebes-dataframe-spark"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0", // % "provided",
  "org.apache.spark" %% "spark-sql" % "2.0.0", // % "provided",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.22"
)

// logLevel in assembly := Level.Debug

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("*.**" -> "cebesshaded.glassfish.@0").
    inLibrary("org.glassfish.hk2.external" % "javax.inject" % "2.4.0-b34"),
  ShadeRule.rename("*.**" -> "cebesshaded.glassfish.@0").
    inLibrary("org.glassfish.hk2.external" % "aopalliance-repackaged" % "2.4.0-b34"),
  ShadeRule.rename("*.**" -> "cebesshaded.beanutilscore8.@0").
    inLibrary("commons-beanutils" % "commons-beanutils-core" % "1.8.0"),
  ShadeRule.rename("*.**" -> "cebesshaded.beanutils7.@0").
    inLibrary("commons-beanutils" % "commons-beanutils" % "1.7.0"),
  ShadeRule.rename("org.apache.commons.logging.**" -> "cebesshaded.logging12.@0").
    inLibrary("commons-logging" % "commons-logging" % "1.2"),
  ShadeRule.rename("org.apache.hadoop.yarn.**" -> "cebesshaded.yarnapi22.@0").
    inLibrary("org.apache.hadoop" % "hadoop-yarn-api" % "2.2.0")
)