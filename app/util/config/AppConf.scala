package util.config

import play.api.Configuration
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

case class AwsCred(key: String, secret: String, region: String)
case class CheckpointsDynamoConf(tableName: String, creds: AwsCred)

case class PipelineReportingConf(sqsUrl: String)

case class ReportingConf(
  pipeline: Option[PipelineReportingConf]
)

case class AppEventsConf(sqs: String)

object AppConf {

//  implicit val phReportingConf = ProductHint.apply[ReportingConf](fieldMapping = ConfigFieldMapping(identity))
//  implicit val phCheckpointsDynamoConf = ProductHint.apply[CheckpointsDynamoConf](fieldMapping = ConfigFieldMapping(identity))
//  implicit val phPipelineReportingConf = ProductHint.apply[PipelineReportingConf](fieldMapping = ConfigFieldMapping(identity))

  implicit def phConf[A] = ProductHint.apply[A](fieldMapping = ConfigFieldMapping(identity))

  implicit class ConfigurationOps(val c: Configuration) extends AnyVal {
    def awsCred: AwsCred = loadAwsCreds(c)
    def reportingConf: ReportingConf = loadReportingConf(c)
    def checkpointsDynamoConf: CheckpointsDynamoConf = loadCheckpointsDynamo(c)
    def appEventsConf: AppEventsConf = loadAppEventsConf(c)
  }

  def loadAwsCreds(c: Configuration): AwsCred =
    ConfigSource.fromConfig(c.underlying.getConfig("aws.creds"))
      .loadOrThrow[AwsCred]

  def loadCheckpointsDynamo(c: Configuration): CheckpointsDynamoConf =
    ConfigSource.fromConfig(c.underlying.getConfig("resources.checkpointsDynamo"))
      .loadOrThrow[CheckpointsDynamoConf]

  def loadReportingConf(c: Configuration): ReportingConf = {
    ConfigSource.fromConfig(c.underlying.getConfig("reporting"))
      .loadOrThrow[ReportingConf]
  }

  def loadAppEventsConf(c: Configuration): AppEventsConf =
    ConfigSource.fromConfig(c.underlying.getConfig("appEvents"))
      .loadOrThrow[AppEventsConf]
}
