import sbt._

object Dependencies {
  val sparkVersion = "2.2.0"

  val sparkCore = "org.apache.spark" %%  "spark-core" % sparkVersion
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion
  val sparkStreaming = "org.apache.spark" %% "spark-streaming" % sparkVersion
  val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
  val sparkStreamingSqlKafka = "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion

  val sparkCassandraConnector = "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.9"

  val playJson = "com.typesafe.play" %%  s"play-json" % "2.6.9"
}