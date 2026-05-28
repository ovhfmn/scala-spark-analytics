ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.17"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-analytics",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "io.delta" %% "delta-spark" % "3.2.0",

      "org.apache.logging.log4j" % "log4j-api" % "2.26.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.26.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.26.0"
    ),

    javaOptions ++= Seq(
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-Dspark.driver.bindAddress=127.0.0.1",
      "-Dspark.driver.port=7077"
    ),
    fork := true,

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.filterDistinctLines
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case x => MergeStrategy.first
    },

    assembly / mainClass := Some("analytics.Main")
  )
