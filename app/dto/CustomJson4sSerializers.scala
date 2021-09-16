package dto.serializers

package v2 {

  object J4s {

    import dto.deployment.streamingjob.v2.{AppConf, FlinkAppConf, SparkAppConf}
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.Serialization.{write}

    object AppConfSerializer extends CustomSerializer[AppConf](format => (
      {
        case o@JObject(list) =>
          implicit val formats = DefaultFormats
          val appType = list.filter(_._1 == "appType")
            .map(_._2)
            .map(_.extract[String])
            .head
          appType match {
            case "spark" => o.extract[SparkAppConf]
            case "flink" => o.extract[FlinkAppConf]
          }
      },
      {
        case conf: SparkAppConf =>
          implicit val formats = DefaultFormats
          write(conf)
        case conf: FlinkAppConf =>
          implicit val formats = DefaultFormats
          write(conf)
      }
    ))

  }
}
