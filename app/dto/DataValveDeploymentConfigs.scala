package dto

package deployment.streamingjob {
  package v1 {
    case class DeploymentCnf(formatVersion: String, appName: String, appVersion: String, cluster: EmrClusterCnf, bootstrapScripts: List[String], sparkAppConf: SparkAppCnf)
    case class EmrClusterCnf(version: String, serviceAccessSecurityGroup: String, ec2SubnetId: String,
      emrManagedMasterSecurityGroup: String, emrManagedSlaveSecurityGroup: String,
      nodes: List[EmrNodeCnf], tags: List[TagCnf])
    case class EmrNodeCnf(role: String, instanceType: String, instanceCount: Int, volumes: List[VolumeCnf])
    case class VolumeCnf(volumeType: String, sizeInGb: Int)
    case class TagCnf(name: String, value: String)
    case class SparkAppCnf(driverMemory: String, conf: List[String], packages: List[String], extraClasspath: ExtraClasspathCnf, args: List[String])
    case class ExtraClasspathCnf(folder: String, jarUrls: List[String])

    case class EmrClusterName(region: String, env: String, version: String, app: String, service: String, group: String)
    case class ArtifactName(app: String, service: String, version: String)
  }

  package v2 {
    case class AppDeploymentConfig(formatVersion: String, appName: String, appVersion: String, cluster: EmrClusterConfig, bootstrapScripts: List[String], appConf: AppConf)
    case class EmrClusterConfig(version: String, serviceAccessSecurityGroup: String, ec2SubnetId: String, emrManagedMasterSecurityGroup: String, emrManagedSlaveSecurityGroup: String, nodes: List[EmrNodeConfig], tags: List[TagConf])
    case class EmrNodeConfig(role: String, instanceType: String, instanceCount: Int, volumes: List[VolumeConf])
    case class VolumeConf(volumeType: String, sizeInGb: Int)
    case class TagConf(name: String, value: String)
    case class ExtraClasspathConf(folder: String, jarUrls: List[String])
    case class EmrClusterName(region: String, env: String, version: String, app: String, service: String, group: String)
    case class ArtifactName(app: String, service: String, version: String)
    trait AppConf
    case class SparkAppConf(appType: String, driverMemory: String, conf: List[String], packages: List[String], extraClasspath: ExtraClasspathConf, args: List[String]) extends AppConf
    case class FlinkAppConf(appType: String, defaultParallelism: Int, conf: List[String], args: List[String]) extends AppConf
  }
}
