name := "hw02"

version := "0.1"

scalaVersion := "2.13.8"


val hadoopClientVersion = "3.3.1"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client"
).map(_ % hadoopClientVersion)


/*assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}*/

/*assemblyMergeStrategy in assembly := {
  case PathList("module-info.class") => MergeStrategy.discard
  case x if x.endsWith("/module-info.class") => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}*/