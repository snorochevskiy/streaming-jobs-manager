package dto

case class ViewEmrAppRestartLog(
  id: Long,
  emrAppId: String,
  clusterId: String,
  redeployTimestamp: String,
  failedStepStart: String,
  failedStepEnd: String,
  reason: String
)

case class ViewEmrAppHealthcheckFailLog(
  id: Long,
  emrAppId: String,
  clusterId: String,
  detectTimestamp: String,
  hcName: String,
  hcLabel: String
)