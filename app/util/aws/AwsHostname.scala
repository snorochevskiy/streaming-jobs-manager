package util.aws

object AwsHostname {

  val IpRegex = """ip-(\d+)-(\d+)-(\d+)-(\d+).*""".r

  /**
   * Parse hostname in format ip-96-113-30-133.ec2.internal and extracts IP 4 address
   * @param awsHostname
   */
  def parseIp4(awsHostname: String): Either[Throwable, String] =
    awsHostname match {
      case IpRegex(d1,d2,d3,d4) => Right(s"$d1.$d2.$d3.$d4")
      case _ => Left(new IllegalArgumentException(s"Unexpected AWS hostname format: $awsHostname"))
    }


}
