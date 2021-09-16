package service

import dao.GenericAppDao
import play.api.Configuration
import util.aws.{EventBridgeMsgParser, SqsClient}

import javax.inject.Inject
import scala.concurrent.ExecutionContext

class AppEventsService @Inject()(
  config: Configuration,
  reporterService: ReporterService,
  genericAppDao: GenericAppDao
)(implicit executionContext: ExecutionContext) {

  import util.config.AppConf._

  /**
   * Fetches new messages from an SQS queue that is connected
   * to the Amazon EventBridge, that sends messages from Amazon ECS.
   * For now it is used to detect the fact, that container has been stopped.
   */
  def processNewEventBridgeMessage() = {
    val events = new SqsClient(config.awsCred).fetch(config.appEventsConf.sqs)
    events.flatMap(EventBridgeMsgParser.parseEventBridgeJson)
      .foreach { e =>
        genericAppDao.createEvent(e)
        reporterService.report(e.appName, e.appName, e.message)
      }
  }

}
