package controllers

import javax.inject.{Inject, Singleton}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import service.EmrService

import scala.concurrent.ExecutionContext

@Singleton
class HelpController @Inject()(
  val controllerComponents: ControllerComponents
)(implicit ec: ExecutionContext) extends BaseController {

  def help() = Action { implicit request: Request[AnyContent] =>
    Ok(views.html.help())
  }

}
