package dao

import com.google.inject.ImplementedBy

import scala.concurrent.Future

@ImplementedBy(classOf[EmrAppDaoSlickImpl])
trait EmrAppDao {

  /**
   * List all application that are to be monitored.
   * @return
   */
  def listEmrApps(): Future[Seq[DbEmrApp]]

  def findEmrApp(emrAppId: String): Future[Option[DbEmrApp]]

  /**
   * Fetches list of discovery rules for an application with given ID.
   * @param emrAppId
   * @return
   */
  def findEmrAppIdentifiers(emrAppId: String): Future[Seq[DbEmrAppIdentifier]]

  /**
   * For a given application, fetches healthiness rules that are based on CloudWatch metrics.
   * @param emrAppId
   * @return
   */
  def findEmrAppCwMetrics(emrAppId: String): Future[Seq[DbEmrAppCwMetric]]

  /**
   * Logs a record indicating that a monitored application has been restarted.
   * @param record
   */
  def logRestart(record: DbEmrAppRestartLog): Unit

  /**
   * Fetches last 50 restart log records.
   * @return
   */
  def listRestartLogs(): Future[Seq[DbEmrAppRestartLog]]

  def logHealthcheckFail(record: EmrAppHealthcheckFailLog): Unit

  /**
   * Fetches last 50 health-check fail log records.
   * @return
   */
  def listHealthcheckFailLog(): Future[Seq[EmrAppHealthcheckFailLog]]
}

@ImplementedBy(classOf[GenericAppDaoSlickImpl])
trait GenericAppDao {

  def listRecentEvents(takeLast: Long = 50L): Future[Seq[GenericAppEvent]]

  def createEvent(event: GenericAppEvent): Unit
}
