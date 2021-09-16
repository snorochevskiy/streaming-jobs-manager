package util.aws

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import util.config.AwsCred

import java.util.UUID
import java.util.stream.Collectors
import scala.jdk.CollectionConverters.CollectionHasAsScala

/**
 * Wrapper for AmazonSQS.
 * @param awsCred
 */
class SqsClient(awsCred: AwsCred) {

  /**
   * Post a text as an SQS message to a queue with the given URL.
   * @param sqsUrl
   * @param text
   */
  def post(sqsUrl: String, text: String): Unit =
    withClient { client =>
      client.sendMessage(sqsUrl, text)
    }

  def fetch(sqsUrl: String): List[String] =
    withClient { client =>
      val resp = client.receiveMessage(sqsUrl)
      val messages = resp.getMessages
      if (!messages.isEmpty) {
        // Acknowledging messages
        val messagesToDelete = resp.getMessages.stream()
          .map(m => new DeleteMessageBatchRequestEntry(UUID.randomUUID().toString,m.getReceiptHandle))
          .collect(Collectors.toList[DeleteMessageBatchRequestEntry])
        client.deleteMessageBatch(sqsUrl, messagesToDelete)

        // Collecting message text bodies
        resp.getMessages.asScala
          .map(_.getBody)
          .toList
      } else Nil
    }

  private def withClient[A](f: AmazonSQS=>A): A = {
    val client = makeClient()
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }

  private def makeClient(): AmazonSQS = {
    AmazonSQSClientBuilder.standard()
      .withRegion(Regions.US_EAST_1)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsCred.key, awsCred.secret)))
      .build()
  }
}
