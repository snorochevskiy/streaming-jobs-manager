package service

import dao.DbEmrAppCwMetric
import javax.inject.Inject
import play.api.Configuration
import util.aws.CloudWatchClient

import scala.concurrent.ExecutionContext

class CloudWatchService @Inject()(config: Configuration)(implicit executionContext: ExecutionContext) {

  import util.config.AppConf._

  /**
   * Takes a set of entries metric_name+rule, for each entry:
   * 1) retrieves recent metric value
   * 2) using the rule, checks if the metric value indicates a healthy state of the application
   *
   * @param metrics list of metric names with rules
   * @return health indicating reports
   */
  def checkCwMetrics(metrics: Seq[DbEmrAppCwMetric]): Seq[HealthIndicator] = {
    val cw = new CloudWatchClient(config.awsCred)

    metrics.map { metric =>
      val metricData = cw.getMetric(metric.nameSpace, metric.metricName, metric.aggFunc, metric.secondsToNow)

      metricData match {
        case Right(v) =>
          val checkResult: Boolean = metric.checkType match {
            case "greater" | "gt" | ">" => v > metric.valueCompareAgainst
            case "less" | "lt" | "<" => v < metric.valueCompareAgainst
            case _ =>
              // TODO: report error
              true
          }
          HealthIndicator(metric.name, s"($v)", checkResult)
        case Left(e) =>
          HealthIndicator(metric.name, e, false)
      }

    }
  }

  def checkStandardEmrCwMetrics(clusterId: String): List[HealthIndicator] = {
    val cw = new CloudWatchClient(config.awsCred)

    val hdfsUtilizationHi = cw.getMetricAvg("AWS/ElasticMapReduce", "HDFSUtilization", 15 * 60, List("JobFlowId"->clusterId)) match {
      case Right(v) => HealthIndicator("HDFS utilization", s"${v.round}%", v < 90.0)
      case Left(e) => HealthIndicator("HDFS utilization", e, false)
    }

    List(
      hdfsUtilizationHi
    )
  }

}
