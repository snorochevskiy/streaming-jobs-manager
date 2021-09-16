package util.aws

import org.scalatest.{MustMatchers, WordSpec}

import scala.io.Source
import scala.util.Using

class EventBridgeMsgParserTest extends WordSpec with MustMatchers {

  "EventBridge parser" must {
    "parse container stopped message" in {
      val text = Using(Source.fromResource("AmazonEventBridge/container_stopped.json"))(_.mkString).get
      val event = EventBridgeMsgParser.parseEventBridgeJson(text)

      event.isDefined mustBe true
      event.get.appName mustBe "service:myapp-0_2_6"
      event.get.eventType mustBe "ECS Task State Change"
      event.get.message mustBe "Task failed ELB health checks in (target-group arn:aws:elasticloadbalancing:us-east-1:111111111111:targetgroup/myapp/fbcb015c4a3ec9f6)"
      event.get.receivedTimestamp.toString mustBe "2021-03-16 03:29:10.0"
    }
  }

}