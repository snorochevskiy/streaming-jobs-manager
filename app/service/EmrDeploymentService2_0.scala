package service

import com.amazonaws.services.elasticmapreduce.model.{Application, BootstrapActionConfig, EbsBlockDeviceConfig, EbsConfiguration, HadoopJarStepConfig, InstanceGroupConfig, InstanceRoleType, JobFlowInstancesConfig, MarketType, RunJobFlowRequest, RunJobFlowResult, StepConfig, Tag, VolumeSpecification}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import dto.deployment.streamingjob.v2.{AppConf, AppDeploymentConfig, FlinkAppConf, SparkAppConf}
import javax.inject.Inject
import play.api.{Configuration, Logger}
import util.aws.EmrClient
import util.spark.SparkSubmitUtil

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.IterableHasAsJava

class EmrDeploymentService2_0 @Inject()(
  config: Configuration,
  reporterService: ReporterService
)(implicit executionContext: ExecutionContext) {

  private implicit val log: Logger = Logger(this.getClass)

  import util.config.AppConf._

  implicit val emr: EmrClient = new EmrClient(config.awsCred)

  val S3BootstrapBucket = "my-emr-job-bootstrap"
  val S3ArtifactsBucket = "my-emr-job-artifacts"

  def doDeployment(artifactName: String, profile: String, depConf: AppDeploymentConfig) = {
    val appProfile = if (System.getProperty("profile") ne null) System.getProperty("profile") else profile
    if (appProfile eq null) throw new RuntimeException("No profile set")
    if (!List("dev","prod").contains(appProfile)) throw new RuntimeException(s"Unknown profile $appProfile")

    val clusterName = s"east-${appProfile}-${depConf.appVersion}.${depConf.appName}.pipeline"

    // Preparing EMR AWS tags
    val NameTag = new Tag("Name", clusterName)
    val CustomApplicationNameTag = new Tag("CustomApplicationName", depConf.appName)

    val tags = depConf.cluster.tags
      .map(t =>  new Tag(t.name, t.value))
      .appended(NameTag)
      .appended(CustomApplicationNameTag)
      .asJavaCollection

    // Artifact should be already deployed to S3
    val artifactS3Url = s"s3://${S3ArtifactsBucket}/${artifactName}"

    // TODO: here validate if such artifact exists in S3
    println("Deploying: " + artifactS3Url)

    //val emr: AmazonElasticMapReduce = emrClient()
    new EmrClient(config.awsCred).withClient { emr =>
      // create a step to enable debugging in the AWS Management Console
      val stepFactory = new StepFactory()
      val enableDebugging = new StepConfig()
        .withName("Enable debugging")
        .withActionOnFailure("TERMINATE_JOB_FLOW")
        .withHadoopJarStep(stepFactory.newEnableDebuggingStep())

      // Application to be installed on EMR
      val emrApplication = requiredApplications(depConf.appConf)

      val appStartCommand = makeAppStartCommand(artifactName, appProfile, depConf.appConf)

      val steps = makePreLaunchSteps(depConf.appConf) ++ List(
        new StepConfig("Download JAR Step", new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs("aws", "s3", "cp", artifactS3Url, "/home/hadoop/")
        ).withActionOnFailure("CONTINUE"),
        new StepConfig("App run step", new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs(appStartCommand: _*)
        ).withActionOnFailure("CONTINUE")
      )

      println("Will run spark using: " + appStartCommand.mkString(" "))

      val clusterNodes = depConf.cluster.nodes.map { n =>
        new InstanceGroupConfig()
          .withName(n.role)
          .withInstanceRole(InstanceRoleType.fromValue(n.role))
          .withInstanceType(n.instanceType)
          .withInstanceCount(n.instanceCount)
          .withEbsConfiguration(new EbsConfiguration()
            .withEbsBlockDeviceConfigs(n.volumes.map { v =>
              new EbsBlockDeviceConfig()
                .withVolumesPerInstance(1)
                .withVolumeSpecification(new VolumeSpecification()
                  .withSizeInGB(v.sizeInGb)
                  .withVolumeType(v.volumeType)
                )
            }.asJavaCollection
            )
          )
          .withMarket(MarketType.ON_DEMAND)
      }

      val instancesConfig = clusterNodes.filter(_.getInstanceCount > 0).toArray

      // launch EMR
      val runJobReq = new RunJobFlowRequest()
        .withVisibleToAllUsers(true)
        .withName(clusterName)
        .withReleaseLabel(depConf.cluster.version)
        .withSteps(enableDebugging)
        .withApplications(emrApplication: _*)
        .withLogUri(s"s3://my-emr-job-logs/${depConf.appName}/$appProfile/logs/")
        .withServiceRole("EMR_IAM_Role")
        .withJobFlowRole("DataPipelineDefaultResourceRole") // custom EMR role for the EC2 instance profile
        .withTags(tags)
        .withConfigurations(createEmrConfigurations(depConf.appConf))
        .withInstances(new JobFlowInstancesConfig()
          .withServiceAccessSecurityGroup(depConf.cluster.serviceAccessSecurityGroup)
          .withEc2SubnetId(depConf.cluster.ec2SubnetId) // Can also use "subnet-e237ba86"
          .withEmrManagedMasterSecurityGroup(depConf.cluster.emrManagedMasterSecurityGroup)
          .withEmrManagedSlaveSecurityGroup(depConf.cluster.emrManagedSlaveSecurityGroup)
          .withEc2KeyName("mykey")
          .withInstanceGroups(instancesConfig:_*)
          .withKeepJobFlowAliveWhenNoSteps(true)
        )
        .withBootstrapActions(
          (
            makeSpecificBootstrapActions(depConf.appConf)
              ++ SparkSubmitUtil.bootstrapScripts(depConf.bootstrapScripts)
          ).asJavaCollection
        )
        .withSteps(steps: _*)

      val runJobResult: RunJobFlowResult = emr.runJobFlow(runJobReq)
      runJobResult.getJobFlowId
    }

  }

  def requiredApplications(appConf: AppConf): Array[Application] =
    Array(
      new Application().withName("Hadoop"),
      appConf match {
        case _: SparkAppConf => new Application().withName("Spark")
        case _: FlinkAppConf => new Application().withName("Flink")
      }
    )

  def makeAppStartCommand(artifactName: String, appProfile: String, appConf: AppConf): List[String] =
    appConf match {
      case sparkConf: SparkAppConf => makeSparkStartCommand(artifactName, appProfile, sparkConf)
      case flinkConf: FlinkAppConf => makeFlinkStartCommand(artifactName, appProfile, flinkConf)
    }

  def makeSparkStartCommand(artifactName: String, appProfile: String, sparkAppConf: SparkAppConf): List[String] =
    List("spark-submit",
      "--master", "yarn",
      "--deploy-mode", "cluster",
      "--driver-memory", sparkAppConf.driverMemory
    ) ++ SparkSubmitUtil.Packages(sparkAppConf.packages.toArray: _*) ++
      SparkSubmitUtil.Drivers(sparkAppConf.extraClasspath.folder, sparkAppConf.extraClasspath.jarUrls) ++
      sparkAppConf.conf.flatMap(a => List("--conf", a)) ++
      List("--conf", s"spark.driver.extraJavaOptions=-Dprofile=$appProfile") ++
      List(s"/home/hadoop/${artifactName}") ++ sparkAppConf.args

  def makeFlinkStartCommand(artifactName: String, appProfile: String, flinkAppConf: FlinkAppConf): List[String] =
    List("flink",
      "run", // //"-m", "yarn-cluster" doesn't work property on EMR - fails with strange Kryo error
      "-p", flinkAppConf.defaultParallelism.toString,
      s"/home/hadoop/${artifactName}"
    ) ++ flinkAppConf.args

  def makeSpecificBootstrapActions(appConf: AppConf): List[BootstrapActionConfig] =
    appConf match {
      case sparkConf: SparkAppConf =>
        List(
          SparkSubmitUtil.bootstrapDownloadLibs(sparkConf.extraClasspath.folder, sparkConf.extraClasspath.jarUrls)
        )
      case flinkConf: FlinkAppConf =>
        Nil
    }

  def createEmrConfigurations(appConf: AppConf): java.util.Collection[com.amazonaws.services.elasticmapreduce.model.Configuration] = {
    import scala.collection.JavaConverters._
    val configurations = new java.util.ArrayList[com.amazonaws.services.elasticmapreduce.model.Configuration]()

    configurations.add(new com.amazonaws.services.elasticmapreduce.model.Configuration()
      .withClassification("capacity-scheduler")
      .withProperties(Map(
        "yarn.scheduler.capacity.resource-calculator" -> "org.apache.hadoop.yarn.util.resource.DominantResourceCalculator"
      ).asJava))

    configurations.add(new com.amazonaws.services.elasticmapreduce.model.Configuration()
      .withClassification("yarn-site")
      .withProperties(Map(
        "yarn.nodemanager.pmem-check-enabled" -> "false",
        "yarn.nodemanager.localizer.cache.cleanup.interval-ms" -> "3600000",
        "yarn.log-aggregation-enable" -> "true",
        "yarn.log-aggregation.retain-seconds" -> "3600",
        "yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds" -> "3600"
      ).asJava))

    appConf match {
      case _: SparkAppConf =>
        configurations.add(new com.amazonaws.services.elasticmapreduce.model.Configuration()
          .withClassification("spark-defaults")
          .withProperties(Map(
            "spark.history.fs.cleaner.enabled" -> "true",
            "spark.history.fs.cleaner.maxAge" -> "2h",
            "spark.history.fs.cleaner.interval" -> "1h"
          ).asJava))
      case _: FlinkAppConf =>
        configurations.add(
          new com.amazonaws.services.elasticmapreduce.model.Configuration()
          .withClassification("flink-conf")
          .withProperties(Map(
            "jobmanager.memory.process.size" -> "1600m",
            "taskmanager.memory.process.size" -> "4096m"
          ).asJava)
        )
        configurations.add(
          new com.amazonaws.services.elasticmapreduce.model.Configuration()
            .withClassification("flink-log4j")
            .withProperties(Map(
              "log4j.appender.file" -> "org.apache.log4j.RollingFileAppender",
              "log4j.appender.file.append" -> "true",
              "log4j.appender.file.MaxFileSize" -> "100MB",
              "log4j.appender.file.MaxBackupIndex" -> "4",
              "log4j.appender.file.layout" -> "org.apache.log4j.PatternLayout",
              "log4j.appender.file.layout.ConversionPattern" -> "%d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n"
            ).asJava)
        )
        // TODO: log4j.properties, flink-log4j-session, log4j-cli.properties
    }

    configurations
  }

  def makePreLaunchSteps(appConf: AppConf): List[StepConfig] = appConf match {
    case _: SparkAppConf => Nil
    case _: FlinkAppConf =>
      List(
        new StepConfig("Flink_Long_Running_Session", new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs("flink-yarn-session", "-d")
        )
      )
  }

}
