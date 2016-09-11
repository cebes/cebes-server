name := "cebes-http-server"

scalastyleConfig := baseDirectory.value / "../build/scalastyle-config.xml"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-http-experimental" % "2.4.10",
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % "2.4.10",
  "com.softwaremill.akka-http-session" %% "core" % "0.2.7",
  "com.softwaremill.akka-http-session" %% "jwt"  % "0.2.7",

  "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2",
  "ch.qos.logback" % "logback-classic" % "1.1.7",

  "com.google.inject" % "guice" % "4.1.0"
)

mainClass in assembly := Some("io.cebes.server.Main")

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
  ShadeRule.rename("org.apache.commons.logging.**" -> "cebesshaded.logging113.@0").
    inLibrary("commons-logging" % "commons-logging" % "1.1.3"),
  ShadeRule.rename("org.apache.hadoop.yarn.**" -> "cebesshaded.yarnapi22.@0").
    inLibrary("org.apache.hadoop" % "hadoop-yarn-api" % "2.2.0"),
  ShadeRule.rename("org.slf4j.impl.**" -> "cebesshaded.logbackslf4j.@0").
    inLibrary("ch.qos.logback" % "logback-classic" % "1.1.7")
)