package dao

import slick.jdbc.JdbcProfile

trait DbEmrTables {

  protected val driver: JdbcProfile
  import driver.api._

  class EmrAppTable(tag: Tag) extends Table[DbEmrApp](tag, "EMR_APPS") {
    def id = column[String]("ID", O.PrimaryKey)
    def name = column[String]("NAME")
    def description = column[String]("DESCRIPTION")
    override def * = (id,name,description) <> (DbEmrApp.tupled, DbEmrApp.unapply)
  }
  val emrApps = TableQuery[EmrAppTable]

  class EmrAppIdentifierTable(tag: Tag) extends Table[DbEmrAppIdentifier](tag, "EMR_APP_IDENTIFIERS") {
    def id = column[String]("ID", O.PrimaryKey)
    def emrAppId = column[String]("EMR_APP_ID")
    def name = column[String]("NAME")
    def awsRegion = column[String]("AWS_REGION")
    def lookupType = column[String]("LOOKUP_TYPE")
    def version = column[String]("VERSION")
    def param1 = column[String]("PARAM1")
    def param2 = column[String]("PARAM2")
    override def * = (id,emrAppId,name,awsRegion,lookupType,version.?,param1.?,param2.?) <> (DbEmrAppIdentifier.tupled, DbEmrAppIdentifier.unapply)
  }
  val emrAppIdentifiers = TableQuery[EmrAppIdentifierTable]

  case class EmrAppCwMetricTable(tag: Tag) extends Table[DbEmrAppCwMetric](tag, "EMR_APP_CW_METRICS") {
    def id = column[String]("ID", O.PrimaryKey)
    def emrAppId = column[String]("EMR_APP_ID")
    def name = column[String]("NAME")
    def awsRegion = column[String]("AWS_REGION")
    def nameSpace = column[String]("NAME_SPACE")
    def metricName = column[String]("METRIC_NAME")
    def aggFunc = column[String]("AGG_FUNC")
    def secondsToNow = column[Int]("SECONDS_TO_NOW")
    def checkType = column[String]("CHECK_TYPE")
    def valueCompareAgainst = column[Double]("VALUE_COMPARE_AGAINST")
    override def * = (id,emrAppId,name,awsRegion,nameSpace,metricName,aggFunc,secondsToNow,checkType,valueCompareAgainst) <> (DbEmrAppCwMetric.tupled, DbEmrAppCwMetric.unapply)
  }
  val emrAppCwMetrics = TableQuery[EmrAppCwMetricTable]

  case class EmrAppRestartLogTable(tag: Tag) extends Table[DbEmrAppRestartLog](tag, "EMR_APP_RESTART_LOGS") {
    def id = column[Long]("ID", O.PrimaryKey)
    def emrAppId = column[String]("EMR_APP_ID")
    def clusterId = column[String]("CLUSTER_ID")
    def redeployTimestamp = column[java.sql.Timestamp]("REDEPLOY_TIMESTAMP")
    def failedStepStart = column[java.sql.Timestamp]("FAILED_STEP_START")
    def failedStepEnd = column[java.sql.Timestamp]("FAILED_STEP_END")
    def reason = column[String]("REASON")
    override def * = (id.?,emrAppId,clusterId,redeployTimestamp,failedStepStart,failedStepEnd,reason) <> (DbEmrAppRestartLog.tupled, DbEmrAppRestartLog.unapply)
  }
  val emrAppRestartLogs = TableQuery[EmrAppRestartLogTable]

  case class EmrAppHealthcheckFailLogTable (tag: Tag) extends Table[EmrAppHealthcheckFailLog](tag, "EMR_APP_HEALTHCHECK_FAIL_LOGS") {
    def id = column[Long]("ID", O.PrimaryKey)
    def emrAppId = column[String]("EMR_APP_ID")
    def clusterId = column[String]("CLUSTER_ID")
    def detectTimestamp = column[java.sql.Timestamp]("DETECT_TIMESTAMP")
    def hcName = column[String]("HC_NAME")
    def hcLabel = column[String]("HC_LABEL")
    override def * = (id.?,emrAppId,clusterId,detectTimestamp,hcName,hcLabel) <> (EmrAppHealthcheckFailLog.tupled, EmrAppHealthcheckFailLog.unapply)
  }
  val emrAppHealthcheckFailLogs = TableQuery[EmrAppHealthcheckFailLogTable]

  case class GenericApplicationEventTable(tag: Tag) extends Table[GenericAppEvent](tag, "GENERIC_APPLICATION_EVENTS") {
    def id = column[Long]("ID", O.PrimaryKey)
    def appName = column[String]("APP_NAME")
    def eventType = column[String]("EVENT_TYPE")
    def message = column[String]("MESSAGE")
    def receivedTimestamp = column[java.sql.Timestamp]("RECEIVED_TIMESTAMP")
    override def * = (id.?,appName,eventType,message,receivedTimestamp) <> (GenericAppEvent.tupled, GenericAppEvent.unapply)
  }
  val genericApplicationEvents = TableQuery[GenericApplicationEventTable]
}
