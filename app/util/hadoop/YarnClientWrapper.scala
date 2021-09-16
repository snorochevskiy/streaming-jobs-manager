package util.hadoop

import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.yarn.api.records.{ApplicationReport, YarnApplicationAttemptState, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class YarnClientWrapper(emrMasterIp: String, port: Int = 8032) {

  def listApplications(): mutable.Buffer[ApplicationReport] =
    withClient { yarnClient =>
      val apps  = yarnClient.getApplications
      apps.asScala
    }

  def killAllApplications(): List[String] =
    withClient { yarnClient =>
      val apps  = yarnClient.getApplications.asScala
        .filter(app => app.getYarnApplicationState == YarnApplicationState.RUNNING)
      apps.foreach(app => yarnClient.killApplication(app.getApplicationId, "Killed by DVMS"))
      apps.map(app => app.getApplicationId.toString).toList
    }

  def withClient[A](f: YarnClient=>A): A = {
    var yarnClient: YarnClient = null
    try {
      val conf = new YarnConfiguration()
      conf.setSocketAddr(YarnConfiguration.RM_ADDRESS, NetUtils.createSocketAddrForHost(emrMasterIp, port))

      yarnClient = YarnClient.createYarnClient()
      yarnClient.init(conf)
      yarnClient.start()

      f(yarnClient)
    } finally {
      if (yarnClient ne null) {
        yarnClient.stop()
        yarnClient.close()
      }
    }
  }
}
