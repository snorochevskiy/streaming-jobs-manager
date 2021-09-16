package dto.serializers.v2.J4s

import dto.deployment.streamingjob.{v1, v2}
import dto.serializers
import dto.serializers.v2.J4s
import org.scalatest.{MustMatchers, WordSpec}

import scala.io.Source
import scala.util.Using

class AppConfSerializerTest extends WordSpec with MustMatchers {

  "deployment service" must {
    "correctly parse deployment config" in {
      val configText = Using.resource(Source.fromResource("deployment_configs/v2/spark_config.json"))(_.mkString)

      import org.json4s._
      import org.json4s.jackson.JsonMethods
      implicit val formats: Formats = DefaultFormats + serializers.v2.J4s.AppConfSerializer

      val jsonObj = JsonMethods.parse(configText)

      val depConf = jsonObj.extract[v2.AppDeploymentConfig]

      depConf.isInstanceOf[v2.AppDeploymentConfig] mustBe true
    }
  }

}
