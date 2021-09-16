package dao

case class DbEmrApp(
  id: String,
  name: String,
  description: String
)

case class DbEmrAppIdentifier(
  id: String,
  emrAppId: String,
  name: String,
  awsRegion: String,
  lookupType: String,
  version: Option[String],
  param1: Option[String],
  param2: Option[String]
)

case class DbEmrAppCwMetric(
  id: String,
  emrAppId: String,
  name: String,
  awsRegion: String,
  nameSpace: String,
  metricName: String,
  aggFunc: String,
  secondsToNow: Int,
  checkType: String,
  valueCompareAgainst: Double
)

case class DbEmrAppRestartLog(
  id: Option[Long],
  emrAppId: String,
  clusterId: String,
  redeployTimestamp: java.sql.Timestamp,
  failedStepStart: java.sql.Timestamp,
  failedStepEnd: java.sql.Timestamp,
  reason: String
)

case class EmrAppHealthcheckFailLog (
  id: Option[Long],
  emrAppId: String,
  clusterId: String,
  detectTimestamp: java.sql.Timestamp,
  hcName: String,
  hcLabel: String
)

case class GenericAppEvent(
  id: Option[Long],
  appName: String,
  eventType: String,
  message: String,
  receivedTimestamp: java.sql.Timestamp
)