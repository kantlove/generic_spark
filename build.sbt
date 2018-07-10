val commonSettings = Seq(
  version := "1.0.0",
  scalaVersion := "2.11.12",

  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots")
  ),

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0",

    // other useful libraries
    "com.chuusai" %% "shapeless" % "2.3.3",
    "com.kailuowang" %% "henkan-convert" % "0.6.2",

    // for testing
    "org.scalacheck" %% "scalacheck" % "1.13.5" % Test,
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.6" % Test
  ),

  // Show more concise errors
  // https://github.com/tek/splain
  addCompilerPlugin("io.tryp" % "splain" % "0.3.1" cross CrossVersion.patch),

  scalacOptions := Seq("-unchecked", "-deprecation"),
)

val generic_spark = (project in file("generic_spark"))
  .settings(commonSettings)
