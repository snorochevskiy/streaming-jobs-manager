package dao

import javax.inject.{Inject, Singleton}
import play.api.db.slick.{DatabaseConfigProvider, HasDatabaseConfigProvider}
import slick.jdbc.JdbcProfile

import scala.concurrent.Future

@Singleton
class EmrAppDaoSlickImpl @Inject()
(
  protected val dbConfigProvider: DatabaseConfigProvider
) extends EmrAppDao with DbEmrTables with HasDatabaseConfigProvider[JdbcProfile] {

  import profile.api._

  override def listEmrApps(): Future[Seq[DbEmrApp]] = db run emrApps.result

  override def findEmrApp(emrAppId: String): Future[Option[DbEmrApp]] =
    db run emrApps.filter(_.id === emrAppId).result.headOption

  override def findEmrAppIdentifiers(emrAppId: String): Future[Seq[DbEmrAppIdentifier]] =
    db run emrAppIdentifiers.filter(_.emrAppId === emrAppId).result

  override def findEmrAppCwMetrics(emrAppId: String): Future[Seq[DbEmrAppCwMetric]] =
    db run emrAppCwMetrics.filter(_.emrAppId === emrAppId).result

  override def logRestart(record: DbEmrAppRestartLog): Unit = {
    db run emrAppRestartLogs.insertOrUpdate(record).transactionally
  }

  override def listRestartLogs(): Future[Seq[DbEmrAppRestartLog]] =
    db run emrAppRestartLogs.sorted(_.redeployTimestamp.desc).take(50).result

  override def logHealthcheckFail(record: EmrAppHealthcheckFailLog): Unit =
    db run emrAppHealthcheckFailLogs.insertOrUpdate(record).transactionally

  override def listHealthcheckFailLog(): Future[Seq[EmrAppHealthcheckFailLog]] =
    db run emrAppHealthcheckFailLogs.sorted(_.detectTimestamp.desc).take(50).result
}

@Singleton
class GenericAppDaoSlickImpl @Inject()
(
  protected val dbConfigProvider: DatabaseConfigProvider
) extends GenericAppDao with DbEmrTables with HasDatabaseConfigProvider[JdbcProfile] {

  import profile.api._

  override def listRecentEvents(takeLast: Long = 50L): Future[Seq[GenericAppEvent]] =
    db run genericApplicationEvents.sorted(_.receivedTimestamp.desc).take(takeLast).result

  override def createEvent(event: GenericAppEvent): Unit =
    db run genericApplicationEvents.insertOrUpdate(event).transactionally

}