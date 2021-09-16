package controllers

import javax.inject.{Inject, Singleton}
import play.api.data.Form
import play.api.data.Forms.{text, tuple}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import service.{DeploymentService, EmrDeploymentService1_0, EmrDeploymentService2_0, EmrService}

import scala.concurrent.ExecutionContext

@Singleton
class DeploymentController @Inject()(
  val controllerComponents: ControllerComponents,
  deploymentService: DeploymentService,
  emrInfoService: EmrService,
  emrDeploymentService1_0: EmrDeploymentService1_0,
  emrDeploymentService2_0: EmrDeploymentService2_0,
)(implicit ec: ExecutionContext) extends BaseController {

  /**
   * Show all available artifacts, so the user can pick up one for deployment.
   */
  def listArtifacts() = Action.async { implicit request: Request[AnyContent] =>
    deploymentService.listDvArtifacts()
      .map(res => Ok(views.html.deployment.listArtifacts(res)))
  }

  /**
   * Shows available deployment config files nested in a selected artifact.
   * @param artifact
   * @return
   */
  def listDeploymentConfiguration(artifact: String)= Action { implicit request: Request[AnyContent] =>
    deploymentService.listDeploymentConfigurations(artifact) match {
      case Right(deploymentConfigs) => Ok(views.html.deployment.listDeployConfigs(artifact, deploymentConfigs))
      case Left(t) =>InternalServerError(t.getMessage)
    }
  }

  /**
   * This step happens when user selects one of deployment config files.
   * The user is provided with a deployment file content, to be able to change some of properties.
   * @param artifact
   * @param cfg
   * @return
   */
  def configureDeployment(artifact: String, cfg: String) = Action { implicit request: Request[AnyContent] =>
    deploymentService.configureDeployment1(artifact, cfg) match {
      case Right(deploymentConfig) => Ok(views.html.deployment.configureDeploy(artifact, deploymentConfig))
      case Left(t) => InternalServerError(t.getMessage)
    }
  }

  val userForm = Form(
    tuple(
      "envSelect" -> text,
      "deploymentConfigJson" -> text
    )
  )

  /**
   * This is the final step of deployment flow.
   * User has selected a configuration file and has submitted it for the deployment.
   * @param artifact
   * @return
   */
  def doDeploy(artifact: String) = Action { implicit request: Request[AnyContent] =>
    val (env, configJsonText) = userForm.bindFromRequest.get

    val clusterId = deploymentService.doDeployment(artifact, env, configJsonText)

    Ok(views.html.deployment.deployStarted(clusterId))
  }

  def autoRestart() = Action.async { implicit request: Request[AnyContent] =>
    emrInfoService.restartApps()
      .map(report => Ok(views.html.restartReport(report)))
  }

  def restartHistory() = Action.async { implicit request: Request[AnyContent] =>
    emrInfoService.listRestartLogs()
      .map(report => Ok(views.html.restartReport(report)))
  }
}
