package controllers

import javax.inject.{Inject, Singleton}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import service.EmrService
import util.hadoop.YarnClientWrapper

import scala.concurrent.ExecutionContext

@Singleton
class TestingController @Inject()(
  val controllerComponents: ControllerComponents,
  emrService: EmrService
)(
  implicit ec: ExecutionContext
) extends BaseController {

  def testStepLog() = Action { implicit request: Request[AnyContent] =>
    val res = emrService.findStepFailReason("s3://my-emr-job-logs/my_app/prod/logs/", "j-CLUSTERID", "s-STEPID")
    Ok(views.html.testing.testingStepLog(res))
  }

  def jarTest() = Action { implicit request: Request[AnyContent] =>
    val res = new YarnClientWrapper("driver_ip", 8032).listApplications()
    Ok(views.html.testing.justText(res.toString()))
  }
}
