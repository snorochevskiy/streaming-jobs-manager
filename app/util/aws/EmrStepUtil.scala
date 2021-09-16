package util.aws

import com.amazonaws.services.elasticmapreduce.model.{HadoopJarStepConfig, KeyValue, StepConfig, StepSummary}

import scala.jdk.CollectionConverters.{IterableHasAsJava, MapHasAsScala}

object EmrStepUtil {
  def cloneStep(step: StepSummary): StepConfig =
    new StepConfig()
      .withName(step.getName)
      .withActionOnFailure(step.getActionOnFailure)
      .withHadoopJarStep(new HadoopJarStepConfig()
        .withJar(step.getConfig.getJar)
        .withArgs(step.getConfig.getArgs)
        .withMainClass(step.getConfig.getMainClass)
        .withProperties(step.getConfig.getProperties.asScala.map(e=> new KeyValue(e._1,e._2)).toList.asJavaCollection)
      )

  def cmdStep(stepName: String, cmd: String*) =
    new StepConfig(stepName, new HadoopJarStepConfig()
      .withJar("command-runner.jar")
      .withArgs(cmd: _*)
    ).withActionOnFailure("CONTINUE")

}
