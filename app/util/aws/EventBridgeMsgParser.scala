package util.aws

import dao.GenericAppEvent
import play.api.libs.json.{JsValue, Json}
import util.date.DateUtil

object EventBridgeMsgParser {

  /**
   * Parses a JSON messages received from the Amazon EventBridge
   * @param text message JSON body
   * @return Some(event) or None in case if we are not interested in a given event
   */
  def parseEventBridgeJson(text: String): Option[GenericAppEvent] = {
    val json = Json.parse(text)
    (json \ "source").toOption.map(_.as[String]) match {
      case Some("aws.ecs") => parseEcsEvent(json)
      case _ => None
    }
  }

  /**
   * Parse a message that had been emitted by Amazon ECS
   * @param json
   * @return
   */
  def parseEcsEvent(json: JsValue): Option[GenericAppEvent] =
    for {
      eventType <- (json \ "detail-type").toOption.map(_.as[String])
      time <- (json \ "time").toOption.map(_.as[String]).flatMap(DateUtil.parse8601ToSqlTimestamp)
      detail <- (json \ "detail").toOption
      service <- (detail \ "group").toOption.map(_.as[String])
      stoppedReason <- (detail \ "stoppedReason").toOption.map(_.as[String])
    } yield GenericAppEvent(None, service, eventType, stoppedReason, time)

}
