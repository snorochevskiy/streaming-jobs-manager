package service.actors

import akka.actor.ActorSystem
import javax.inject.{Inject, Singleton}
import play.api.{Logger, inject}
import play.api.inject.{ApplicationLifecycle, SimpleModule}
import service.EmrService

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, DurationInt}

@Singleton
class HealthCheckScheduler @Inject()(
  actorSystem: ActorSystem,
  lifecycle: ApplicationLifecycle,
  emrInfoService: EmrService
) (implicit ec: ExecutionContext) {

  val scheduledTick = actorSystem.scheduler.schedule(Duration.Zero, 60.second) {
    Logger("HealthCheckScheduler").info("Autorestarting failed applications")
    emrInfoService.restartApps()
  }
}

class HealthCheckSchedulerModule extends SimpleModule(inject.bind[HealthCheckScheduler].toSelf.eagerly())
