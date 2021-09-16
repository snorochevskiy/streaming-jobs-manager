package service

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.{Callable, TimeUnit}

import cats.data.{EitherT, OptionT}
import cats.instances.future._
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxOptionId}
import com.amazonaws.services.elasticmapreduce.model.{Application, Cluster, ClusterSummary, EbsBlockDeviceConfig, EbsConfiguration, HadoopJarStepConfig, InstanceGroup, InstanceGroupConfig, InstanceRoleType, JobFlowInstancesConfig, KeyValue, MarketType, RunJobFlowRequest, RunJobFlowResult, StepConfig, StepSummary, Tag, VolumeSpecification}
import com.amazonaws.services.s3.AmazonS3URI
import com.google.common.cache.{Cache, CacheBuilder}
import dao.{DbEmrApp, DbEmrAppIdentifier, DbEmrAppRestartLog, EmrAppDao, EmrAppHealthcheckFailLog}
import dto.{ViewEmrAppHealthcheckFailLog, ViewEmrAppRestartLog}
import javax.inject.Inject
import play.api.{Configuration, Logger}
import util.aws.{AwsHostname, EmrClient, EmrStepUtil, S3Client}
import util.hadoop.HdfsClientWrapper
import util.logs.StepLogsProcessor

import scala.concurrent.{ExecutionContext, Future}

class EmrService @Inject()(
  config: Configuration,
  cloudWatchService: CloudWatchService,
  emrAppDao: EmrAppDao,
  reporterService: ReporterService
)(implicit executionContext: ExecutionContext) {

  private implicit val log: Logger = Logger(this.getClass)

  val cache: Cache[String,Seq[DvAppHealth]] = CacheBuilder.newBuilder()
    .expireAfterWrite(1, TimeUnit.MINUTES)
    .build()

  import util.config.AppConf._

  implicit val emr: EmrClient = new EmrClient(config.awsCred)

  def getEmrAppsDetailedInfo(): Future[Seq[EmrApp]] = {
    emrAppDao.listEmrApps()
      .flatMap(dbApps =>
        Future.sequence(dbApps.map(getEmrAppHealthInfo(_)))
      )
  }

  def getEmrAppDetails(appId: String): Future[Option[EmrApp]] =
    OptionT(emrAppDao.findEmrApp(appId))
      .semiflatMap(getEmrAppHealthInfo)
      .value

  def findEmrMasterIp(appId: String): Future[Either[Throwable,String]] =
    EitherT(
      emrAppDao.findEmrAppIdentifiers(appId)
        .map(findClusterWithIdentifiers(_).toRight(new RuntimeException(s"Cannot find running EMR for $appId")))
    )
      .map(c => emr.describeCluster(c.getId))
      .subflatMap(c => emrMasterIp(c))
      .value

  def getEmrAppHealthInfo(dbApp: DbEmrApp)(implicit emr: EmrClient = new EmrClient(config.awsCred)): Future[EmrApp] =
    for {
      emrIdentifiers <- emrAppDao.findEmrAppIdentifiers(dbApp.id)
      cwHealthMetrics <- emrAppDao.findEmrAppCwMetrics(dbApp.id)
    } yield {
      val opCluster = findClusterWithIdentifiers(emrIdentifiers)
      EmrApp(dbApp.id, dbApp.name,
        opCluster.map(c => toEmrApp(c, emr.listGroups(c.getId))),
        cloudWatchService.checkCwMetrics(cwHealthMetrics) ++
          opCluster.map(c => cloudWatchService.checkStandardEmrCwMetrics(c.getId)).getOrElse(Nil)
      )
    }

  def findClusterWithIdentifiers(is: Seq[DbEmrAppIdentifier]): Option[ClusterSummary] = {
    is.to(LazyList)
      .flatMap(findClusterByIdentifier)
      .headOption
  }

  def findClusterByIdentifier(i: DbEmrAppIdentifier): Option[ClusterSummary] = {
    i.lookupType.toLowerCase match {
      case "emr_name_regex" =>
        val emr = new EmrClient(config.awsCred)
        emr.findClusterByNameRegex(i.param1.get) // FIXME - option
      case _ => None
    }
  }

  /**
   * Iterates through all monitored applications and checks their status.
   * If an application is down, then tries to restart it.
   * @return
   */
  def restartApps(): Future[Seq[ViewEmrAppRestartLog]] = {
    val dbApps: Future[Seq[(DbEmrApp,Seq[DbEmrAppIdentifier])]] = emrAppDao.listEmrApps()
      .flatMap{ apps =>
        Future.sequence(apps.map(app => emrAppDao.findEmrAppIdentifiers(app.id).map(indentifs => (app, indentifs))))
      }

    val runningApps = dbApps.map(_.flatMap { case (app, indentifs) => findClusterWithIdentifiers(indentifs).map(app -> _) })

    val restarted = runningApps.map( _
      .filter { case (app, cluster) => cluster.getStatus.getState.toUpperCase contains "WAITING" }
      .flatMap { case (app, cluster) =>restartFailedApp(app, cluster) }
    )

    runningApps.map( _
      .filter { case (app, cluster) => cluster.getStatus.getState.toUpperCase contains "RUNNING" }
      .map { case (app, cluster) => checkAppHealth(app, cluster) }
    )

    restarted
  }

  def restartFailedApp(app: DbEmrApp, cluster: ClusterSummary) =
    emr.listSteps(cluster.getId).headOption.flatMap {step => // steps are in reversed order
      if (step.getStatus.getState.toUpperCase contains "FAILED") {

        val clusterDetails = emr.describeCluster(cluster.getId)
        val failReason = findStepFailReason(clusterDetails.getLogUri, cluster.getId, step.getId) match {
          case Right(msg) => msg
          case Left(e) => log.warn("Unable identify fail reason", e); s"unknown: ${e.getMessage}"
        }
        val healingApplied = tryToRecover(failReason, clusterDetails)

        val clonedStep = EmrStepUtil.cloneStep(step)
        emr.addStep(cluster.getId, clonedStep)

        val dbLog = makeRestartLogEntity(app, cluster, step, failReason)
        emrAppDao.logRestart(dbLog)
        reporterService.report(app.name, cluster.getId,
          s"Restarting application. Healing applied: ${healingApplied}. Fail reason: $failReason"
        )
        toViewEntity(dbLog).some
      } else None
    }

  val failedHealthIndicatorsCache: Cache[(String,String),HealthIndicator] = CacheBuilder.newBuilder()
    .expireAfterWrite(30, TimeUnit.MINUTES)
    .build()

  def checkAppHealth(dbApp: DbEmrApp, cluster: ClusterSummary) = {
    emrAppDao.findEmrAppCwMetrics(dbApp.id)
      .map { cwHealthMetrics =>
        val customMetrics = cloudWatchService.checkCwMetrics(cwHealthMetrics)
        val standardEmrCwMetrics = cloudWatchService.checkStandardEmrCwMetrics(cluster.getId)
        (customMetrics ++ standardEmrCwMetrics)
          .filter(! _.healthy)
          .foreach { hi =>
            failedHealthIndicatorsCache.get(cluster.getId -> hi.name, () => {
              emrAppDao.logHealthcheckFail(makeHealthcheckFailLogEntity(dbApp, cluster, hi))
              reporterService.report(dbApp.name, s"${cluster.getName} at ${cluster.getId}", s"${hi.name}: ${hi.label}")
              hi
            })
          }
      }

  }

  def listRestartLogs(): Future[Seq[ViewEmrAppRestartLog]] = {
    emrAppDao.listRestartLogs()
      .map(_.map(toViewEntity))
  }

  def listHealthcheckLogs(): Future[Seq[ViewEmrAppHealthcheckFailLog]] = {
    emrAppDao.listHealthcheckFailLog()
      .map(_.map(toViewEntity))
  }

  /**
   * Fetches stderr log for given clusterId and stepId and tries to find a cause of Spark application crush.
   *
   * s3://my-emr-project-logs/app_name/prod/logs/CLUSTER_ID/steps/STEP_ID/stderr.gz
   *
   * @param s3BasePath base path of S3 folder for keeping logs; during the cluster creating is passed as logUri
   * @param clusterId ID of EMR cluster
   * @param stepId ID of EMR step
   * @return
   */
  def findStepFailReason(s3BasePath: String, clusterId: String, stepId: String): Either[Exception, String] = {
    val stderrArchive = s"${s3BasePath}${clusterId}/steps/${stepId}/stderr.gz"
    val uri = new AmazonS3URI(stderrArchive.replace("s3n:", "s3:"))
    log.info(s"Will process step log file ${uri.getKey} from bucket ${uri.getBucket}")
    val failMessage = new S3Client(config.awsCred).readAsGZippedTextStream(uri.getBucket, uri.getKey) { reader =>
      StepLogsProcessor.parseLastStackTrace(StepLogsProcessor.collectStackTraceChunks(reader))
        .map(lastStackTrace => lastStackTrace.frames.last.message)
    }
    failMessage
  }

  /**
   * Checks whether given fail reason requires an auto-healing, and applies it if possible.
   * @param failReason
   * @param cluster
   */
  def tryToRecover(failReason: String, cluster: Cluster): Unit = {
    if (failReason contains "org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block") {
      emr.addStep(cluster.getId, EmrStepUtil.cmdStep("Clear HDFS checkpoints", "hadoop", "fs", "-rm", "-r", "/streaming-jobs-checkpoints"))
    }
  }


  private def makeRestartLogEntity(dbEmrApp: DbEmrApp, cluster: ClusterSummary, step: StepSummary, reason: String): DbEmrAppRestartLog =
    DbEmrAppRestartLog(None, dbEmrApp.id, cluster.getId, new Timestamp(new Date().getTime),
      new Timestamp(step.getStatus.getTimeline.getStartDateTime.getTime),
      new Timestamp(step.getStatus.getTimeline.getEndDateTime.getTime),
      reason
    )

  private def makeHealthcheckFailLogEntity(dbEmrApp: DbEmrApp, cluster: ClusterSummary, healthIndicator: HealthIndicator): EmrAppHealthcheckFailLog =
    EmrAppHealthcheckFailLog(
      None, dbEmrApp.id, cluster.getId, new Timestamp(new Date().getTime), healthIndicator.name, healthIndicator.label
    )
  private def toViewEntity(dbLog: DbEmrAppRestartLog): ViewEmrAppRestartLog = {
    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ssXXX")
    ViewEmrAppRestartLog(
      id = dbLog.id.getOrElse(0L),
      emrAppId = dbLog.emrAppId,
      clusterId = dbLog.clusterId,
      redeployTimestamp = df.format(dbLog.redeployTimestamp),
      failedStepStart = df.format(dbLog.failedStepStart),
      failedStepEnd = df.format(dbLog.failedStepEnd),
      reason = dbLog.reason
    )
  }

  private def toViewEntity(dbHcLog: EmrAppHealthcheckFailLog): ViewEmrAppHealthcheckFailLog = {
    val df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ssXXX")
    ViewEmrAppHealthcheckFailLog(
      id = dbHcLog.id.getOrElse(0L),
      emrAppId = dbHcLog.emrAppId,
      clusterId = dbHcLog.clusterId,
      detectTimestamp = df.format(dbHcLog.detectTimestamp),
      hcName = dbHcLog.hcName,
      hcLabel =  dbHcLog.hcLabel
    )
  }

  private def toEmrApp(c: ClusterSummary, instanceGroups: List[InstanceGroup]) =
    EmrDetails(
      id = c.getId,
      name = c.getName,
      status = EmrStatus(c.getStatus.getState, c.getStatus.getTimeline.getCreationDateTime),
      instanceGroups = instanceGroups
    )


  private def emrMasterIp(cluster: Cluster): Either[Throwable,String] = {
    AwsHostname.parseIp4(cluster.getMasterPublicDnsName)
  }

  private def hdfsClient(cluster: Cluster) = {
    // ip-96-113-30-133.ec2.internal
    AwsHostname.parseIp4(cluster.getMasterPublicDnsName)
      .map(HdfsClientWrapper(_))
      .right.get
  }
}

case class EmrApp(
  id: String,
  name: String,
  cluster: Option[EmrDetails],
  healthIndicators: Seq[HealthIndicator] = Seq.empty
)

case class EmrDetails(
  id: String,
  name: String,
  status: EmrStatus,
  instanceGroups: Seq[InstanceGroup] = Seq.empty
)

case class EmrStatus(
  state: String,
  creationDate: java.util.Date
)

case class EmrInstance(
  id: String
)

case class DvAppHealth(
  app: String,
  healthIndicators: Seq[HealthIndicator]
)

case class HealthIndicator(
  name: String,
  label: String,
  healthy: Boolean
)
