package dto

trait HealthCheck
case class CloudWatchCmpCheck(id: String, name: String, cond: Condition) extends HealthCheck

trait Condition {

}

case class CmpCondition(namespace: String, metric: String, cmp: String)
case class ComplexCondition(combinator: String, conds: Condition*)

