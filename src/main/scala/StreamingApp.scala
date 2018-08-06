
import java.lang
import java.sql.Timestamp
import java.time.Instant

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.collection.mutable
import scala.util.{Failure, Success, Try}


object StreamingApp {

  private val log = LoggerFactory.getLogger(this.getClass)

  val DevelopmentMode = true

  val BotMaxBlockTimeMs = 10 * 60 * 1000
  val MaxEventLagMs = 30 * 1000

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
  val KafkaConsumer = "spark_consumer"

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(createSsc)
    ssc.start()
    ssc.awaitTermination()
  }

  def createSsc(): StreamingContext = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("StreamingApp")
      .set("spark.cassandra.connection.host", CassandraHost)
      .set("spark.cassandra.connection.port", CassandraPort)

    val ssc = new StreamingContext(conf, Seconds(30))
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

    val prevOffsets = mutable.HashSet[OffsetRange]()

    stream
      .map(kafkaRecord2CaseClass)
      .filter(_.nonEmpty)
      .map(_.get)
      .window(Minutes(11), Seconds(30))
      .foreachRDD( processRdd(_, cc) )

    val offsets = mutable.Set[Tuple2[Long, Seq[OffsetRange]]] ()

    stream.foreachRDD { (rdd, time) =>
      val currentOffsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsets += ( (time.milliseconds, currentOffsets) )
      def currentTimestamp: Long = Instant.now().toEpochMilli
      val committed = offsets
        .filter {
          case (timestamp, _) =>
            if ( (currentTimestamp - timestamp) > (BotMaxBlockTimeMs + MaxEventLagMs) ) true
            else false
        }
        .map {
          case (timestamp, offsetRanges) =>
            log.info(s"Committing offset ranges: ${offsetRanges.toList}")
            stream.asInstanceOf[CanCommitOffsets]
              .commitAsync(offsetRanges.toArray)
            (timestamp, offsetRanges)
        }
      offsets --= committed
    }

    ssc
  }

  def kafkaRecord2CaseClass(consumerRecord: ConsumerRecord[String, String]): Option[Event] = Try({
    val json = Json.parse(consumerRecord.value())
    val categoryId = (json \ "category_id").get.as[Int]
    val timestamp = new Timestamp((json \ "unix_time").get.as[Long])
    val ip = (json \ "ip").get.as[String]
    val eventType = (json \ "type").get.as[String]
    Event(ip, timestamp, categoryId, eventType)
  }) match {
    case Success(x) => Some(x)
    case Failure(f) =>
      log.warn(s"Couldn't parse a row: ${consumerRecord.value()}", f)
      None
  }

  def processRdd(rdd: RDD[Event], cc: CassandraConnector): Unit = {
    def currentTimestamp: Long = Instant.now().toEpochMilli

    rdd
      .filter {
        case Event(_, timestamp, _, _) =>
          if ((currentTimestamp - timestamp.getTime) < BotMaxBlockTimeMs) true
          else false
      }
      .map {
        case event @ Event(ip, timestamp, categoryId, eventType) => (ip, event)
      }
      .groupByKey()
      .map { case (ip, events) =>
        var eventCount = 0
        var categories = mutable.Set[Int]()
        var clickCount = 0
        for (event <- events) {
          eventCount += 1
          categories += event.categoryId
          clickCount += {
            if (event.eventType == "click") 1 else 0
          }
        }
        val clickVewRatio = if (eventCount == clickCount) 0 else 1.0 * clickCount / (eventCount - clickCount)
        (ip, eventCount, categories.size, clickVewRatio)
      }
      .filter {
        case (ip, eventCount, categoriesCount, clickVewRatio) => eventCount > BotMaxEventThreshold ||
          categoriesCount > BotMaxCategoriesThreshold ||  clickVewRatio > BotMaxClickVewRatio
      }
      .foreachPartition(iter => {
        cc.withSessionDo(session => {
          val preparedStmnt = session.prepare(CassandraSqlInsert)
          iter.foreach { case (ip, eventCount, categoriesCount, clickVewRatio) =>
            log.info(s"Adding a bot to Cassandra " +
              s"ip: $ip eventCount: $eventCount categoriesCount: $categoriesCount clickVewRatio: $clickVewRatio")
            session.execute(
              preparedStmnt.bind(
                ip,
                new java.lang.Integer(eventCount),
                new lang.Float(clickVewRatio),
                new java.lang.Integer(categoriesCount)
              )
            )
          }
        })
      })
  }


}

case class Event(ip: String, timestamp: Timestamp, categoryId: Int, eventType: String)


