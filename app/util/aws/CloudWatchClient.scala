package util.aws

import java.util.Date
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClient}
import com.amazonaws.services.cloudwatch.model.{Dimension, GetMetricDataRequest, GetMetricStatisticsRequest, Metric, MetricDataQuery, MetricStat}
import play.api.Logging
import util.config.AwsCred

import scala.collection.JavaConverters.seqAsJavaListConverter

/**
 * Wrapper for AmazonCloudWatch
 * @param awsCred
 */
class CloudWatchClient(awsCred: AwsCred) extends Logging {

  var cw: AmazonCloudWatch = AmazonCloudWatchClient.builder()
    .withRegion(awsCred.region)
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsCred.key, awsCred.secret)))
    .build()

  def getMetricAvg(namespace: String, name: String, durationSec: Int, dimensions: List[(String,String)] = Nil): Either[String,Double] =
    getMetric(namespace, name, "Average", durationSec, dimensions)

  def getMetricSum(namespace: String, name: String, durationSec: Int, dimensions: List[(String,String)] = Nil): Either[String,Double] =
    getMetric(namespace, name, "Sum", durationSec, dimensions)
  /**
   * Queries Cloudwatch for a metric value aggregated on a given interval.
   * @param namespace CloudWatch namespace
   * @param name metric name
   * @param durationSec last second to aggregate on
   * @return aggregated metric value
   */
  def getMetric(namespace: String, name: String, aggFunc: String, durationSec: Int, dimensions: List[(String,String)] = Nil): Either[String,Double] = {

    val agg = aggFunc.toLowerCase match{
      case "sum" => "Sum"
      case "avg" | "average" => "Average"
      case _ => throw new IllegalArgumentException(s"Unknown aggregation function $aggFunc")
    }

    //TODO: Add support for AVG aggregation
    val now = new Date()
    val start = new Date(now.getTime - durationSec * 1000)

    dimensions.map{case (n, v) => new Dimension().withName(n).withValue(v)}.asJava

    val metric = new Metric()
      .withNamespace(namespace)
      .withMetricName(name)
      .withDimensions(dimensions.map{case (n, v) => new Dimension().withName(n).withValue(v)}.asJava)

    val stat = new MetricStat()
      .withMetric(metric)
      .withPeriod(durationSec)
      .withStat(agg)

    val metricQuery = new MetricDataQuery()
      .withId("m1")
      .withMetricStat(stat)
      .withReturnData(true)

    val req = new GetMetricDataRequest()
      .withMetricDataQueries(metricQuery)
      .withStartTime(start)
      .withEndTime(now)

    val startTime = System.currentTimeMillis()
    val res = cw.getMetricData(req)
    val endTime = System.currentTimeMillis()
    logger.debug(s"Request $namespace::$name took ${endTime - startTime} milliseconds")

    val metricDataResults = res.getMetricDataResults
    if (metricDataResults.isEmpty) Left("No metric data available")
    else if (metricDataResults.get(0).getValues.isEmpty) Left("No datapoints in result")
    else Right(res.getMetricDataResults.get(0).getValues.get(0))
  }



}
