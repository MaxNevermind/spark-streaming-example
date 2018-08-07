import java.lang
import java.sql.Timestamp
import java.time.Instant

import StreamingApp.{CassandraSqlInsert, createSsc, jsonString2Event, log}
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory

import scala.collection.mutable


object StructStreamingApp extends Utils {

  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  val DevelopmentMode = true

  val BotMaxBlockTimeMs = 10 * 60 * 1000

  val BotMaxEventThreshold = 20
  val BotMaxClickVewRatio = 5
  val BotMaxCategoriesThreshold = 5

  val CassandraHost = if (DevelopmentMode) "localhost" else "cassandra"
  val CassandraPort = "9042"
  val CassandraSqlInsert = s"""
                              |INSERT INTO standalone.blocked_ips (ip, request_count, click_view_ratio, categories_count)
                              |  VALUES(?, ?, ?, ?)
                              |USING TTL ${BotMaxBlockTimeMs / 1000};
                           """.stripMargin

  val KafkaBroker = if (DevelopmentMode) "localhost:29092" else "broker:9092"
  val KafkaTopic = "incoming-events"

  val CheckpointPath = "/Users/mkonstantinov/Downloads/checkpoint/"


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.driver.memory", "2g")
      .appName("spark_local")
      .enableHiveSupport()
      .getOrCreate()
    val cc = CassandraConnector(spark.sparkContext)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val in = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", KafkaBroker)
      .option("subscribe", KafkaTopic)
      .option("startingOffsets", "earliest")
      .load()
      .map(record => {
        jsonString2Event(new String(record.getAs[Array[Byte]]("value"))).orNull
      })
      .filter(_ != null)
      .filter("current_timestamp > timestamp") // Filter out wrong rows so it can't break a watermark

    val out = in
      .withWatermark("timestamp", "11 minutes")
      .groupBy(window($"timestamp", "10 minutes", "30 seconds"), $"ip")
      .agg(
        count($"categoryId").as("eventCount"),
        approx_count_distinct($"categoryId").as("categoriesCount"),
        sum(when($"eventType" === "click", 1).otherwise(0)).as("clickEventCount"),
        sum(when($"eventType" === "view", 1).otherwise(0)).as("viewEventCount")
      )
      .as[EventAgg]
      .filter( _ match {
        case EventAgg(ip, eventCount, categoriesCount, clickEventCount, viewEventCount) =>
          eventCount > BotMaxEventThreshold ||
            categoriesCount > BotMaxCategoriesThreshold ||
            (viewEventCount != 0 && (clickEventCount /  viewEventCount) > BotMaxClickVewRatio)
      })
      .writeStream
      .outputMode(OutputMode.Update())
      .option("checkpointLocation", CheckpointPath)
      .foreach(new ForeachWriter[EventAgg] {
        var session: Session = _
        def open(partitionId: Long, version: Long): Boolean = {
          session = cc.openSession()
          true
        }
        def process(record: EventAgg): Unit = {
          val preparedStmnt = session.prepare(CassandraSqlInsert)
          record match { case EventAgg(ip, eventCount, categoriesCount, clickEventCount, viewEventCount) =>
            val clickVewRatio = if (viewEventCount == 0) 0 else 1.0 * (clickEventCount / viewEventCount)
            log.info(s"Adding a bot to Cassandra " +
              s"ip: $ip eventCount: $eventCount categoriesCount: $categoriesCount clickVewRatio: $clickVewRatio")
            session.execute(
              preparedStmnt.bind(
                ip,
                new java.lang.Integer(eventCount.toInt),
                new lang.Float(clickVewRatio),
                new java.lang.Integer(categoriesCount.toInt)
              )
            )
          }
        }
        def close(errorOrNull: Throwable): Unit = {}
      })
      .start()

    out.awaitTermination()

  }

}

case class EventAgg(ip: String, eventCount: Long, categoriesCount: Long, clickEventCount: Long, viewEventCount: Long)


