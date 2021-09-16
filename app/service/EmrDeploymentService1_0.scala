package service

import com.amazonaws.services.elasticmapreduce.model.{Application, EbsBlockDeviceConfig, EbsConfiguration, HadoopJarStepConfig, InstanceGroupConfig, InstanceRoleType, JobFlowInstancesConfig, MarketType, RunJobFlowRequest, RunJobFlowResult, StepConfig, Tag, VolumeSpecification}
import com.amazonaws.services.elasticmapreduce.util.StepFactory
import dto.deployment.streamingjob.v1.DeploymentCnf
import javax.inject.Inject
import play.api.{Configuration, Logger}
import util.aws.EmrClient
import util.spark.SparkSubmitUtil._

import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.IterableHasAsJava

class EmrDeploymentService1_0 @Inject()(
  config: Configuration,
  reporterService: ReporterService
)(implicit executionContext: ExecutionContext) {

  private implicit val log: Logger = Logger(this.getClass)

  import util.config.AppConf._

  implicit val emr: EmrClient = new EmrClient(config.awsCred)

  val S3BootstrapBucket = "my-emr-job-bootstrap"
  val S3ArtifactsBucket = "my-emr-job-artifacts"

  def doDeployment(artifactName: String, profile: String, depConf: DeploymentCnf) = {
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
      val spark = new Application().withName("Spark")
      val hadoop = new Application().withName("Hadoop")

      val sparkSubmitCommand: List[String] = List("spark-submit",
        "--master", "yarn",
        "--deploy-mode", "cluster",
        "--driver-memory", depConf.sparkAppConf.driverMemory
      ) ++ Packages(depConf.sparkAppConf.packages.toArray: _*) ++
        Drivers(depConf.sparkAppConf.extraClasspath.folder, depConf.sparkAppConf.extraClasspath.jarUrls) ++
        depConf.sparkAppConf.conf.flatMap(a => List("--conf", a)) ++
        List("--conf", s"spark.driver.extraJavaOptions=-Dprofile=$appProfile") ++
        List(s"/home/hadoop/${artifactName}") ++ depConf.sparkAppConf.args

      println("Will run spark using: " + sparkSubmitCommand.mkString(" "))

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
        .withApplications(spark,hadoop)
        .withLogUri(s"s3://my-emr-job-logs/${depConf.appName}/$appProfile/logs/")
        .withServiceRole("EMR_IAM_Role")
        .withJobFlowRole("DataPipelineDefaultResourceRole") // custom EMR role for the EC2 instance profile
        .withTags(tags)
        .withConfigurations(createEmrConfigurations())
        .withInstances(new JobFlowInstancesConfig()
          .withServiceAccessSecurityGroup(depConf.cluster.serviceAccessSecurityGroup)
          .withEc2SubnetId(depConf.cluster.ec2SubnetId) // Can also use "subnet-e237ba86"
          .withEmrManagedMasterSecurityGroup(depConf.cluster.emrManagedMasterSecurityGroup)
          .withEmrManagedSlaveSecurityGroup(depConf.cluster.emrManagedSlaveSecurityGroup)
          .withEc2KeyName("mykey")
          .withInstanceGroups(instancesConfig:_*)
          .withKeepJobFlowAliveWhenNoSteps(true)
        )
        .withBootstrapActions((List(
          bootstrapDownloadLibs(depConf.sparkAppConf.extraClasspath.folder, depConf.sparkAppConf.extraClasspath.jarUrls))
            ++ bootstrapScripts(depConf.bootstrapScripts)
          ).asJavaCollection
        )

        .withSteps(
          new StepConfig("Download JAR Step", new HadoopJarStepConfig()
            .withJar("command-runner.jar")
            .withArgs("aws", "s3", "cp", artifactS3Url, "/home/hadoop/")
          ).withActionOnFailure("CONTINUE"),
          new StepConfig("Spark Step", new HadoopJarStepConfig()
            .withJar("command-runner.jar")
            .withArgs(sparkSubmitCommand:_*)
          ).withActionOnFailure("CONTINUE")
        )

      val runJobResult: RunJobFlowResult = emr.runJobFlow(runJobReq)
      runJobResult.getJobFlowId
    }

  }

}
