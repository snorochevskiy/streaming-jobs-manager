package service

import cats.data.EitherT
import cats.implicits.catsSyntaxEitherId
import javax.inject.Inject
import play.api.{Configuration, Logger}
import util.aws.{DynamoDbWrapper, S3Client}
import util.config.AppConf.ConfigurationOps
import util.hadoop.{HdfsClientWrapper, YarnClientWrapper}

import scala.concurrent.{ExecutionContext, Future}

class HealingService @Inject()(
  config: Configuration,
  emrService: EmrService
)(implicit executionContext: ExecutionContext) {

  private implicit val logger: Logger = Logger(this.getClass)

  val checkpointsDynamoConf = config.checkpointsDynamoConf
  implicit val dynamo = new DynamoDbWrapper(config.awsCred)

  def appNameInDynamo(appId: String) = {
    // TODO-XXX: need to find a way to extract these records from running apps
    if (appId contains "cisco") "MyApp-cisco"
    else if (appId contains "ciena") "MyApp-ciena"
    else if (appId contains "nokia") "MyApp-nokia"
    else if (appId contains "snmp_dx") "MyApp-SnmpDx$"
    else if (appId contains "snmp_sevone") "MyApp-SnmpSevOne$"
    else if (appId contains "soam") "MyApp-Soam"
    else if (appId contains "syslog") "MyApp-Syslog"
    else throw new IllegalArgumentException(s"No DynamoDb checkpoint records for $appId")
  }

  def clearCheckpoints(appId: String): Future[Either[Throwable,ResetCheckpointsReport]] = {
    // Oh, boy... this is not beautiful
    val pathEth = if (appId contains "syslog-dev") "syslog/dev".asRight
    else if (appId contains "syslog-prod") "syslog/prod".asRight
    else if (appId contains "snmp_sevone-dev") "syslog/prod".asRight
    else if (appId contains "snmp_sevone-prod") "syslog/prod".asRight
    else if (appId contains "snmp_dx-dev") "syslog/prod".asRight
    else if (appId contains "snmp_dx-prod") "syslog/prod".asRight
    else if (appId contains "soam-dev") "soam/dev".asRight
    else if (appId contains "soam-prod") "soam/prod".asRight
    else if (appId contains "cisco-dev") "cisco/dev".asRight
    else if (appId contains "cisco-prod") "cisco/prod".asRight
    else if (appId contains "ciena-dev") "ciena/dev".asRight
    else if (appId contains "ciena-prod") "ciena/prod".asRight
    else if (appId contains "nokia-dev") "nokia/dev".asRight
    else if (appId contains "nokia-prod") "nokia/prod".asRight
    else Left("Not checkpoints directory for " + appId)

    EitherT(emrService.findEmrMasterIp(appId))
      .map{ ip =>
        logger.info("Going to clear checkpoints for " + ip)

        val killedAppIds = new YarnClientWrapper(ip).killAllApplications()
        logger.info(s"On $ip killed applications: ${killedAppIds.mkString(",")}")

        val deletedRecords = dynamo.delete(checkpointsDynamoConf.tableName, "Application", appNameInDynamo(appId))

        // TODO: put directory name into environment properties of spark job
        pathEth.map(path => new S3Client(config.awsCred).delete("my-emr-app-checkpoints", path))

        val hdfsCheckpointDir = "/my-emr-app-checkpoints"
        val deletedFromHdfs = HdfsClientWrapper(ip).deleteIfExists(hdfsCheckpointDir)
        logger.info(s"For $ip deleted checkpoint from HDFS: $deletedFromHdfs")

        ResetCheckpointsReport(appId, ip, killedAppIds, (checkpointsDynamoConf.tableName -> deletedRecords), hdfsCheckpointDir + " : " + deletedFromHdfs)
      }
      .value
  }

}

case class ResetCheckpointsReport(
  appId: String,
  masterIp: String,
  killedAppIds: List[String],
  removedDynamoRecords: (String, List[String]),
  clearedHdfsFile: String
)