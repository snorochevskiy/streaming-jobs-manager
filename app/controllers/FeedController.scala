package controllers

import dao.GenericAppDao
import javax.inject.{Inject, Singleton}
import play.api.mvc.{AnyContent, BaseController, ControllerComponents, Request}
import service.EmrService

import scala.concurrent.ExecutionContext

@Singleton
class FeedController @Inject()(
  val controllerComponents: ControllerComponents,
  emrInfoService: EmrService,
  genericAppDao: GenericAppDao
)(implicit ec: ExecutionContext) extends BaseController {

  /**
   * Returns RSS feed for Streaming Jobs Applications log
   */
  def emrRestartsHistory() = Action.async { implicit request: Request[AnyContent] =>
    emrInfoService.listRestartLogs().map(data =>
      Ok(
        <rss version="2.0">
          <channel>
            <title>EMR apps restart history</title>
            <link>{routes.HomeController.index().absoluteURL()}</link>
            <description>Crushed application restart logs</description>
            {for (item <- data) yield {
              <item>
                <title>{item.emrAppId} ({item.clusterId})</title>
                <link>{routes.ManagedAppsController.listApplications().absoluteURL()}</link>
                <description>{item.reason}</description>
                <guid isPermaLink="false">{item.id}</guid>
                <pubDate>{item.redeployTimestamp}</pubDate>
              </item>
            }}
          </channel>
        </rss>
      ))
  }

  def healthcheckFailsHistory() = Action.async { implicit request: Request[AnyContent] =>
    emrInfoService.listHealthcheckLogs().map(data =>
      Ok(
        <rss version="2.0">
          <channel>
            <title>EMR apps health check fails history</title>
            <link>{routes.HomeController.index().absoluteURL()}</link>
            <description>Failed health checks logs</description>
            {for (item <- data) yield {
            <item>
              <title>{item.emrAppId} ({item.clusterId}) - {item.hcName} : {item.hcLabel}</title>
              <link>{routes.ManagedAppsController.appDetails(item.emrAppId).absoluteURL()}</link>
              <description>Healthcheck {item.hcName} has failed at {item.detectTimestamp}</description>
              <guid isPermaLink="false">{item.id}</guid>
              <pubDate>{item.detectTimestamp}</pubDate>
            </item>
          }}
          </channel>
        </rss>
      ))
  }

  def genericAppEvents() = Action.async { implicit request: Request[AnyContent] =>
    genericAppDao.listRecentEvents().map(event =>
      Ok(
        <rss version="2.0">
          <channel>
            <title>EMR apps health check fails history</title>
            <link>{routes.HomeController.index().absoluteURL()}</link>
            <description>Applications events</description>
            {for (item <- event) yield {
            <item>
              <title>{item.appName} : {item.eventType}</title>
              <link>{routes.HomeController.index().absoluteURL()}</link>
              <description>{item.message}</description>
              <guid isPermaLink="false">{item.id.getOrElse(0)}</guid>
              <pubDate>{item.receivedTimestamp}</pubDate>
            </item>
          }}
          </channel>
        </rss>
      ))
  }

}
