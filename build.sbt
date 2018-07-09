val commonSettings = Seq(
  version := "1.0.0",
  scalaVersion := "2.11.12",

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0",

    // other useful libraries
    "com.kailuowang" %% "henkan-convert" % "0.6.2",

    // for testing
    "org.scalacheck" %% "scalacheck" % "1.13.5" % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.6" % Test
  ),

  // fork in run := true,

  // assemblyMergeStrategy in assembly := {
  //   case PathList("META-INF", xs @ _*) => MergeStrategy.discard

  //   case x: Any =>
  //     val oldStrategy = (assemblyMergeStrategy in assembly).value
  //     oldStrategy(x)
  // },

  // disable test when package
  // test in assembly := {},

  scalacOptions := Seq("-unchecked", "-deprecation"),

  // Spark depends on an ancient version of Shapeless, shade it so a newer version
  // can be used
  // assemblyShadeRules in assembly := Seq(
  //   ShadeRule.rename("shapeless.**" -> "shadeshapless.@1").inAll
  // )
)

val generic_spark = (project in file("generic_spark"))
  .settings(commonSettings)
  // .settings(
  //   mainClass in assembly := Some(s"generic_spark.Main"),
  //   assemblyJarName in assembly := s"generic_spark.jar"
  // )
