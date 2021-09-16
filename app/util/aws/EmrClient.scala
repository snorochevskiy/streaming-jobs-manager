package util.aws

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.elasticmapreduce.model.{AddJobFlowStepsRequest, Cluster, ClusterState, ClusterSummary, DescribeClusterRequest, DescribeStepRequest, Instance, InstanceGroup, ListClustersRequest, ListInstanceGroupsRequest, ListInstancesRequest, ListStepsRequest, Step, StepConfig, StepSummary}
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClient}
import util.config.AwsCred

import scala.jdk.CollectionConverters._

/**
 * Wrapper for AmazonElasticMapReduce.
 * @param awsCred
 */
class EmrClient(awsCred: AwsCred) {

  /**
   * Queries AWS EMR service to get a list of EMR cluster in WAITING state.
   * Since our deployment approach assumes that Spark applications are launched using EMR step,
   * if an application on a cluster is running, then the cluster is in RUNNING state.
   * But if the application has crushed, then the cluster will be in WAITING state.
   * @return list of clusters in WAITING state
   */
  def listWaitingClusters(): List[ClusterSummary] =
    withClient { client =>
      client.listClusters(new ListClustersRequest().withClusterStates(ClusterState.WAITING)).getClusters.asScala.toList
    }

  /**
   * Queries AWS EMR service to get a list of EMR cluster that are active, i.e. not terminated.
   * @return
   */
  def listNonTerminatedClusters(): List[ClusterSummary] =
    withClient { client =>
      client.listClusters(
        new ListClustersRequest()
          .withClusterStates(
            ClusterState.RUNNING, ClusterState.WAITING, ClusterState.STARTING, ClusterState.BOOTSTRAPPING
          )
      ).getClusters().asScala.toList
    }

  /**
   * Searches an active EMR cluster that has name that fits given regular expression.
   * @param nameRegex regex pattern to test EMR cluster name
   * @return a cluster or None
   */
  def findClusterByNameRegex(nameRegex: String): Option[ClusterSummary] = {
    val re = nameRegex.r
    listNonTerminatedClusters()
      .find(c => re.findFirstIn(c.getName).isDefined)
  }

  /**
   * Just a wrapper for AmazonElasticMapReduce.describeCluster
   * @param clusterId ID of ERM cluster
   * @return cluster details
   */
  def describeCluster(clusterId: String): Cluster =
    withClient { client =>
      val result = client.describeCluster(new DescribeClusterRequest().withClusterId(clusterId))
      result.getCluster()
    }


  /**
   * Queries instances in a given EMR cluster.
   * @param clusterId EMR cluster ID
   * @return list of instance details
   */
  def listInstances(clusterId: String): List[Instance] =
    withClient { client =>
      val result = client.listInstances(new ListInstancesRequest().withClusterId(clusterId))
      result.getInstances.asScala.toList
    }

  /**
   * Queries instance groups in a given EMR cluster.
   * @param clusterId EMR cluster ID
   * @return list of EMR instance group details
   */
  def listGroups(clusterId: String): List[InstanceGroup] =
    withClient { client =>
      val result = client.listInstanceGroups(new ListInstanceGroupsRequest().withClusterId(clusterId))
      result.getInstanceGroups.asScala.toList
    }

  /**
   * Fetches all EMR Steps for given cluster
   * @param clusterId ID of EMR cluster
   * @return list of step details
   */
  def listSteps(clusterId: String): List[StepSummary] =
    withClient { client =>
      val result = client.listSteps(new ListStepsRequest().withClusterId(clusterId))
      result.getSteps.asScala.toList
    }

  /**
   * Fetches complete information about given EMR step
   * @param clusterId ID of EMR cluster the step belongs to
   * @param stepId ID of Step
   * @return step details
   */
  def describeStep(clusterId: String, stepId: String): Step =
    withClient { client =>
      val result = client.describeStep(new DescribeStepRequest().withClusterId(clusterId).withStepId(stepId))
      result.getStep
    }

  /**
   * Add given steps to an existing EMR cluster.
   * If the cluster in WAITING state, it will make cluster to immediately execute the first step in the list.
   * Otherwise all steps are added as pending.
   * @param clusterId ID of EMR cluster
   * @param steps collection of EMR steps
   * @return
   */
  def addStep(clusterId: String, steps: StepConfig*) =
    withClient { client =>
      val req = new AddJobFlowStepsRequest()
        .withJobFlowId(clusterId)
        .withSteps(steps:_*)
      client.addJobFlowSteps(req)
    }

  def withClient[A](expr: AmazonElasticMapReduce => A): A = {
    val client = makeClient()
    try {
      expr(client)
    } finally {
      client.shutdown()
    }
  }

  private def makeClient(): AmazonElasticMapReduce = 
    AmazonElasticMapReduceClient.builder()
      .withRegion(awsCred.region)
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsCred.key, awsCred.secret)))
      .build()

}
