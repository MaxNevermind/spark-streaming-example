
import java.lang
import java.time.Instant

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object StreamingApp {

  private val log = LoggerFactory.getLogger(this.getClass)

  val BotMaxBlockTimeMs = 24 * 60 * 60 * 1000
  val BotMaxEventThreshold = 20
  val BotMaxClickVewRatio = 5
  val BotMaxCategoriesThreshold = 5

  val CassandraHost = "cassandra"
//  val CassandraHost = "localhost"
  val CassandraPort = "9042"
  val CassandraSqlInsert = s"""
                             |INSERT INTO standalone.blocked_ips (ip, request_count, click_view_ratio, categories_count)
                             |  VALUES(?, ?, ?, ?)
                             |USING TTL ${BotMaxBlockTimeMs / 1000};
                           """.stripMargin

  val KafkaBroker = "broker:9092"
//  val KafkaBroker = "localhost:29092"
  val KafkaTopic = "incoming-events"
  val KafkaConsumer = "spark_consumer"



  def createSsc(): StreamingContext = {

    log.warn("----------------- KafkaBroker - " + KafkaBroker)

    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("StreamingApp")
      .set("spark.cassandra.connection.host", CassandraHost)
      .set("spark.cassandra.connection.port", CassandraPort)

    val ssc = new StreamingContext(conf, Seconds(5))
    val cc = CassandraConnector(ssc.sparkContext)
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KafkaBroker,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> KafkaConsumer,
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(KafkaTopic), kafkaParams)
    )

    def getCurrentTimestamp: Long = Instant.now().toEpochMilli

    stream
      .map(_.value()) // To avoid "object not serializable" SparkException, because Kafka ConsumerRecord in not serializable
      .window(Seconds(20), Seconds(10))
      //      .window(Minutes(10), Minutes(1))
      .map(jsonString => Try({
        val json = Json.parse(jsonString)
        val categoryId = (json \ "category_id").get.as[Int]
        val timestamp = (json \ "unix_time").get.as[Long]  * 1000
        val ip = (json \ "ip").get.as[String]
        val eventType = (json \ "type").get.as[String]
        if ((getCurrentTimestamp - timestamp)  < BotMaxBlockTimeMs)
          Some(ip, Event(categoryId, eventType))
        else {
          log.info(s"Encountered an old record, batch time: $getCurrentTimestamp, event time: $timestamp")
          None
        }
      }) match {
        case Success(Some(x)) => Some(x)
        case Success(None) => None
        case Failure(f) =>
          log.warn(s"Couldn't parse a row: $jsonString", f)
          None
      })
      .filter(_.nonEmpty)
      .map(_.get)
      .groupByKey()
      .map { case (ip, events) =>
        var eventCount = 0
        var categories = mutable.Set[Int]()
        var clickCount = 0
        for (event <- events) {
          eventCount += 1
          categories += event.categoryId
          clickCount += { if (event.eventType == "click") 1 else 0 }
        }
        val clickVewRatio = if (eventCount == clickCount) 0 else 1.0 * clickCount / (eventCount - clickCount)
        (ip, eventCount, categories.size, clickVewRatio)
      }
      .filter {
        case (ip, eventCount, categoriesCount, clickVewRatio) => eventCount > BotMaxEventThreshold ||
          categoriesCount > BotMaxCategoriesThreshold ||  clickVewRatio > BotMaxClickVewRatio
      }
      .mapPartitions(
        iter => {
          cc.withSessionDo(session => {
            var iterResult: Iterator[Object] = null
              val prepared = session.prepare(CassandraSqlInsert)
              iter.map { case (ip, eventCount, categoriesCount, clickVewRatio) =>
                log.info(s"Adding a bot to Cassandra " +
                  s"ip: $ip eventCount: $eventCount categoriesCount: $categoriesCount clickVewRatio: $clickVewRatio")
                session.execute(
                  prepared.bind(
                    ip,
                    new java.lang.Integer(eventCount),
                    new lang.Float(clickVewRatio),
                    new java.lang.Integer(categoriesCount)
                  )
                )
                (ip, eventCount, categoriesCount, clickVewRatio)
              }
          })
        },
        preservePartitioning = true
      )
      .print()

    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      log.info(s"Committing offset ranges: ${offsetRanges.toList}")
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    }

    ssc
  }


  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createSsc)
    ssc.start()
    ssc.awaitTermination()
  }

}

case class Event(categoryId: Int, eventType: String)

