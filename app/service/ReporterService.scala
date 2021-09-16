package service

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import javax.inject.Inject
import play.api.Configuration
import util.aws.SqsClient
import util.config.AppConf.ConfigurationOps
import util.config.PipelineReportingConf

import scala.concurrent.ExecutionContext

class ReporterService @Inject()(config: Configuration)(implicit executionContext: ExecutionContext) {

  /**
   * Report about the problem in all possible ways
   * @param application name of unhealthy application
   * @param instance unhealthy node
   * @param message text message describing the problem
   */
  def report(application: String, instance: String, message: String): Unit = {
    val rc = config.reportingConf

    rc.pipeline.foreach { case PipelineReportingConf(sqsUrl) =>
      val fancyConfigUiFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
      val dateStr = fancyConfigUiFormat.format(new Date())

      val sanitizedText = message.replaceAll("\n", "\\n").replaceAll("\"", "\\\"")

      val msg =
        s"""
           |{
           |  "id": "${UUID.randomUUID().toString}",
           |  "subject": "$application error",
           |  "message": "${sanitizedText}",
           |  "host": "${instance}",
           |  "indexTime": "${dateStr}"
           |}
           |""".stripMargin

      new SqsClient(config.awsCred).post(sqsUrl, msg)

    }

    // TODO: emails


  }

}
