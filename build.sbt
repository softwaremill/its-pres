name := "ITS Akka"

version := "1.0"

scalaVersion := "2.11.6"

val akkaVersion = "2.4.8"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion

libraryDependencies += "com.github.pathikrit" %% "better-files" % "2.14.0"

val flinkVersion = "1.0.3"
libraryDependencies ++= Seq("org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-clients" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion)