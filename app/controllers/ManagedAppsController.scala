package controllers

import javax.inject.{Inject, Singleton}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import service.{EmrService, HealingService, ResetCheckpointsReport}

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class ManagedAppsController @Inject()(
  val controllerComponents: ControllerComponents,
  emrHealthCheckService: EmrService,
  healingService: HealingService
)(implicit ec: ExecutionContext) extends BaseController {

  def listApplications() = Action.async { implicit request: Request[AnyContent] =>
    emrHealthCheckService.getEmrAppsDetailedInfo()
      .map(data => Ok(views.html.apps.clusters(data)))
  }

  def appDetails(appId: String) = Action.async { implicit request: Request[AnyContent] =>
    emrHealthCheckService.getEmrAppDetails(appId)
      .map(data => Ok(views.html.apps.appDetails(data.get)))
  }

  def clearCheckpoints(appId: String) = Action.async { implicit request: Request[AnyContent] =>
    healingService.clearCheckpoints(appId)
      .map {
        case Right(res) => Ok(views.html.apps.checkpointClearReport(res))
        case Left(err) => throw err
      }
  }
}
