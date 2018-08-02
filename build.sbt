
import sbt.Keys.libraryDependencies
import Dependencies._
import sbtassembly.MergeStrategy


name := "streaming_task"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.11.11"


dependencyOverrides += "com.fasterxml.jackson.core" %  "jackson-core" % "2.6.5"
dependencyOverrides += "com.fasterxml.jackson.core" %  "jackson-databind" % "2.6.5"

//val scope = "compile"
val scope = "provided"

libraryDependencies += sparkCore % scope
libraryDependencies += sparkSql % scope
libraryDependencies += sparkHive % scope
libraryDependencies += sparkStreaming % scope

libraryDependencies += sparkStreamingKafka
libraryDependencies += playJson
libraryDependencies += sparkCassandraConnector


val defaultMergeStrategy: String => MergeStrategy = {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case _ => MergeStrategy.first
}

assemblyMergeStrategy in assembly := defaultMergeStrategy

