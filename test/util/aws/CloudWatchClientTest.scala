package util.aws

import org.scalatest.{MustMatchers, WordSpec}
import util.config.AwsCred

class CloudWatchClientTest extends WordSpec with MustMatchers {

  "CloudWatch client" must {
    "get sum metric" in {
      val cred = AwsCred("XXXXXXXXXXXXXXXXXXXX", "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY", "us-east-1")

      val c = new CloudWatchClient(cred)

      val res = c.getMetricSum("my-test-namespace", "batchInputRowCount", 60 * 60)
      println(res)
    }

    "get avg metric with dimension" in {
      val cred = AwsCred("XXXXXXXXXXXXXXXXXXXX", "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY", "us-east-1")

      val c = new CloudWatchClient(cred)

      val res = c.getMetricAvg("AWS/ElasticMapReduce", "HDFSUtilization",  5 * 60, List("JobFlowId"->"j-2CR8XNCIBW7R6"))
      println(res)
    }
  }

}
