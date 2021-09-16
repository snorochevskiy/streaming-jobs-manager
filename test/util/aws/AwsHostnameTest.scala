package util.aws

import org.scalatest.{MustMatchers, WordSpec}

class AwsHostnameTest extends WordSpec with MustMatchers {

  "AWS hostname util" must {
    "parse ip4 address" in {
      AwsHostname.parseIp4("ip-96-113-30-133.ec2.internal") mustBe Right("96.113.30.133")
    }
  }

}
