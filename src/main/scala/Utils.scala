import java.sql.Timestamp

import org.slf4j.LoggerFactory
import play.api.libs.json.Json

import scala.util.{Failure, Success, Try}

trait Utils {

  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)

  def jsonString2Event(jsonString: String): Option[Event] = Try({
    val json = Json.parse(jsonString)
    val categoryId = (json \ "category_id").get.as[Int]
    val timestamp = new Timestamp((json \ "unix_time").get.as[Long])
    val ip = (json \ "ip").get.as[String]
    val eventType = (json \ "type").get.as[String]
    Event(ip, timestamp, categoryId, eventType)
  }) match {
    case Success(x) => Some(x)
    case Failure(f) =>
      log.warn(s"Couldn't parse a row: $jsonString", f)
      None
  }


}
