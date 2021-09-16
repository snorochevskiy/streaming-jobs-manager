package controllers

import java.sql.Timestamp

import dao.{GenericAppDao, GenericAppEvent}
import javax.inject.{Inject, Singleton}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import play.api.libs.json._
import play.api.libs.functional.syntax._

import scala.concurrent.ExecutionContext

/**
 * This controller is used to accept events about monitored applications from other systems.
 * e.g. from SNS, EventBridge and other applications that can perform HTTP call.
 */
@Singleton
class EventsController @Inject()(
  val controllerComponents: ControllerComponents,
  genericAppDao: GenericAppDao
)(implicit ec: ExecutionContext) extends BaseController {


  implicit val GenericEventReader = (
    (__ \ "applicationName").read[String] and
      (__ \ "eventType").read[String] and
      (__ \ "message").read[String]
    ).tupled

  // TODO-XXX: not exposed yet
  def createAppEvent() = Action { implicit request: Request[AnyContent] =>
    request.body.asJson.map { json =>
      json.validate[(String, String, String)].map {
        case (applicationName, eventType, message) =>
          genericAppDao.createEvent(
            GenericAppEvent(None, applicationName, eventType, message, new Timestamp(new java.util.Date().getTime))
          )
          Created
      }.recoverTotal {
        e => BadRequest("Detected error: " + JsError.toFlatForm(e))
      }
    }.getOrElse {
      BadRequest("Expecting Json data")
    }
  }

}
