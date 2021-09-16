package service.actors

import akka.actor.ActorSystem
import play.api.inject
import play.api.inject.SimpleModule
import service.AppEventsService

import javax.inject.Inject
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt

class AppEventsTask @Inject() (
  actorSystem: ActorSystem,
  appEventsService: AppEventsService
)(implicit executionContext: ExecutionContext) {

  actorSystem.scheduler.scheduleAtFixedRate(initialDelay = 30.seconds, interval = 1.minute) { () =>
    actorSystem.log.info("Checking for messages in SQS")
    appEventsService.processNewEventBridgeMessage()
  }

}

class AppEventsSchedulerModule extends SimpleModule(inject.bind[AppEventsTask].toSelf.eagerly())
