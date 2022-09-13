name := "hw01"

version := "0.1"

scalaVersion := "2.13.8"


val circeVersion = "0.14.1"
//val jsoniterVersion = "2.17.1"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

/*libraryDependencies ++= Seq(
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core",
  // Use the "provided" scope instead when the "compile-internal" scope is not supported
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" // % "compile-internal"
).map(_ % jsoniterVersion)*/