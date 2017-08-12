name := "heatmaps"

version := "1.0"

scalaVersion := "2.12.2"
val Http4sVersion = "0.15.11a"
val circeVersion = "0.8.0"
val rhoVersion = "0.15.0a"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies += "com.github.mauricio" % "postgresql-async_2.12" % "0.2.21"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.1.9"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "com.google.maps" % "google-maps-services" % "0.1.20"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.mockito" % "mockito-all" % "2.0.2-beta"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.google.apis" % "google-api-services-fusiontables" % "v2-rev18-1.21.0"
libraryDependencies += "com.google.oauth-client" % "google-oauth-client-java6" % "1.22.0"
libraryDependencies += "com.google.oauth-client" % "google-oauth-client-jetty" % "1.22.0"
libraryDependencies += "com.github.cb372" % "scalacache-core_2.12" % "0.9.3"
libraryDependencies += "com.github.cb372" %% "scalacache-guava" % "0.9.3"
libraryDependencies += "com.github.davidb" % "metrics-influxdb" % "0.9.3"
libraryDependencies += "nl.grons" %% "metrics-scala" % "3.5.9_a2.4"

libraryDependencies ++= Seq(
  "org.http4s"     %% "http4s-blaze-server" % Http4sVersion,
  "org.http4s"     %% "http4s-circe"        % Http4sVersion,
  "org.http4s"     %% "http4s-twirl"        % Http4sVersion,
  "org.http4s"     %% "http4s-dsl"          % Http4sVersion
)

lazy val root = (project in file(".")).enablePlugins(SbtTwirl)