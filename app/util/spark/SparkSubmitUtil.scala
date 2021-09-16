package util.spark

import java.util

import com.amazonaws.services.elasticmapreduce.model.{BootstrapActionConfig, Configuration, ScriptBootstrapActionConfig}

object SparkSubmitUtil {

  /**
   * Creates entries in spark-submit command to pass given libraries in extraClassPath.
   */
  object Drivers {
    def apply(folder: String, jarUrls: List[String]): List[String] = {
      val fullPaths = jarUrls.map(jarUrl => s"${folder}/${jarNameFromUrl(jarUrl)}")
      "--conf" :: ("spark.driver.extraClassPath=" + fullPaths.mkString(":")) ::
        "--conf" :: ("spark.executor.extraClassPath=" + fullPaths.mkString(":")) ::
        Nil
    }
    def jarNameFromUrl(jarUrl: String): String = {
      val re = """.+\/([-a-zA-Z0-9.]+\.jar)""".r
      val re(fileName) = jarUrl
      fileName
    }
  }

  /**
   * Creates entries in spark-submit command to pass given libraries as packages that are automatically
   * fetched by Spark from maven central.
   */
  object Packages {
    def apply(list: String*): List[String] = {
      "--packages" :: list.mkString(",") :: Nil
    }
  }

  def createEmrConfigurations(): util.Collection[Configuration] = {
    import scala.collection.JavaConverters._
    val configurations = new util.ArrayList[Configuration]()

    configurations.add(new Configuration()
      .withClassification("capacity-scheduler")
      .withProperties(Map(
        "yarn.scheduler.capacity.resource-calculator" -> "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
      ).asJava))

    configurations.add(new Configuration()
      .withClassification("yarn-site")
      .withProperties(Map(
        "yarn.nodemanager.pmem-check-enabled" -> "false",
        "yarn.nodemanager.localizer.cache.cleanup.interval-ms" -> "3600000",
        "yarn.log-aggregation-enable" -> "true",
        "yarn.log-aggregation.retain-seconds" -> "3600",
        "yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds" -> "3600"
      ).asJava))

    configurations.add(new Configuration()
      .withClassification("spark-defaults")
      .withProperties(Map(
        "spark.history.fs.cleaner.enabled" -> "true",
        "spark.history.fs.cleaner.maxAge" -> "2h",
        "spark.history.fs.cleaner.interval" -> "1h"
      ).asJava))

    configurations
  }

  def bootstrapDownloadLibs(localFolder: String, libUrls: List[String]): BootstrapActionConfig =
    new BootstrapActionConfig() //
      .withName("Fetch library JARs")
      .withScriptBootstrapAction(new ScriptBootstrapActionConfig()
        .withPath("s3://my-emr-job-bootstrap/download_libs_by_url.sh")
        .withArgs((localFolder :: libUrls).toArray: _*)
      )

  def bootstrapScripts(urls: List[String]): List[BootstrapActionConfig] =
    urls.map { url =>
      new BootstrapActionConfig() //
        .withName("Bootstrap script")
        .withScriptBootstrapAction(new ScriptBootstrapActionConfig()
          .withPath(url)
        )
    }

}
