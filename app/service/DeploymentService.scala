package service

import cats.implicits.catsSyntaxOptionId
import com.amazonaws.services.elasticmapreduce.model.Tag

import language.postfixOps
import dto.DeploymentOption
import dto.deployment.streamingjob._
import dto.deployment.streamingjob.v1.DeploymentCnf
import dto.serializers
import javax.inject.Inject
import play.api.{Configuration, Logger}
import util.aws.S3Client
import util.config.AppConf.ConfigurationOps
import util.jar.JarHelper

import scala.concurrent.{ExecutionContext, Future}

class DeploymentService @Inject()(
  config: Configuration,
  emrDeploymentService1_0: EmrDeploymentService1_0,
  emrDeploymentService2_0: EmrDeploymentService2_0,
)(implicit executionContext: ExecutionContext) {

  // TODO: move to configs
  val MyAppArtifactsBucket = "my-emr-job-artifacts"

  def listDvArtifacts(): Future[List[DeploymentOption]] = {
    val s3Client = new S3Client(config.awsCred)
    val res = s3Client.listFiles(MyAppArtifactsBucket, "") // FIXME: move to config
      .map(obj => DeploymentOption(obj.getKey, obj.getSize))
    Future.successful(res)
  }

  def listDeploymentConfigurations(artifact: String): Either[Throwable,List[String]] = {
    new S3Client(config.awsCred)
      .readJarStream(MyAppArtifactsBucket, artifact)(JarHelper.listFilesInFolder(_, "_deployment"))
  }

  def configureDeployment1(artifact: String, file: String): Either[Throwable,String] = {
    new S3Client(config.awsCred)
      .readJarStream(MyAppArtifactsBucket, artifact)(JarHelper.readFileInFolder(_, "_deployment", file))
      .map(_.toRight(new IllegalArgumentException(s"Cannot find file $file in artifact $artifact"))).flatten
  }

  // Maybe someday we'll create a beautiful page for configuring the deployment.
  def configureDeployment2(artifact: String): Either[Throwable,List[v1.DeploymentCnf]] = {
    val eitherDeploymentFiles = new S3Client(config.awsCred)
      .readJarStream(MyAppArtifactsBucket, artifact)(JarHelper.readFilesInFolder(_, "_deployment"))

    eitherDeploymentFiles.map(_.flatMap{ case(fineName, text) =>
      import org.json4s._
      import org.json4s.jackson.JsonMethods._
      implicit val jsonFormats = org.json4s.DefaultFormats
      val json = parse(text)
      (json \ "formatVersion").extract[String] match {
        case "1.0" =>
          json.extract[v1.DeploymentCnf].some
        case _ =>
          Logger(classOf[DeploymentService]).warn(s"Unable to parse deployment config file $fineName in artifact $artifact")
          None
      }
    })
  }

  def doDeployment(artifact: String, env: String, depConfText: String): String = {
    import org.json4s._
    import org.json4s.jackson.JsonMethods

    implicit val formats: Formats = DefaultFormats + serializers.v2.J4s.AppConfSerializer

    val jsonObj = JsonMethods.parse(depConfText)

    val version = (jsonObj \ "formatVersion").extract[String]

    version match {
      case "1.0" =>
        val v1DepConf = jsonObj.extract[v1.DeploymentCnf]
        emrDeploymentService1_0.doDeployment(artifact, env, v1DepConf)
      case "2.0" =>
        val v2DepConf = jsonObj.extract[v2.AppDeploymentConfig]
        emrDeploymentService2_0.doDeployment(artifact, env, v2DepConf)
    }
  }

}
