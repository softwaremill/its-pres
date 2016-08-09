name := "ITS"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.11.8",
  libraryDependencies += "com.github.pathikrit" %% "better-files" % "2.16.0"
)

lazy val its = (project in file("."))
  .settings(commonSettings)
  .aggregate(akka, flink)

lazy val common = (project in file("common"))
  .settings(commonSettings)

val akkaVersion = "2.4.9-RC2"
lazy val akka = (project in file("akka"))
  .settings(commonSettings)
  .settings(
    libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion,
    libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
  )
  .dependsOn(common)

val flinkVersion = "1.1.0"
lazy val flink = (project in file("flink"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" %% "flink-clients" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)
  )
  .dependsOn(common)

val kafkaVersion = "0.10.0.0"
lazy val kafka = (project in file("kafka"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % kafkaVersion,
      "org.apache.kafka" % "kafka-streams" % kafkaVersion,
      "net.manub" %% "scalatest-embedded-kafka" % "0.7.0")
  )
  .dependsOn(common)

val sparkVersion = "2.0.0"
lazy val spark = (project in file("spark"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion)
  )
  .dependsOn(common)
