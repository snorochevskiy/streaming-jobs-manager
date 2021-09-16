name := """streaming-jobs-manager"""
organization := "snorochevskiy"

version := "1.3"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.13.3"

libraryDependencies += guice
libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "8.0.20",
  "com.amazonaws" % "aws-java-sdk-emr" % "1.11.855",
  "com.amazonaws" % "aws-java-sdk-cloudwatch" % "1.11.855",
  "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.855",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.855",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.855",

  "com.typesafe.play" %% "play-slick" % "4.0.2",
  "com.typesafe.play" %% "play-slick-evolutions" % "4.0.2",

  "com.github.pureconfig" %% "pureconfig" % "0.13.0",
  "org.json4s" %% "json4s-jackson" % "3.6.10",

  "org.apache.hadoop" % "hadoop-client" % "3.2.1",

  "org.typelevel" %% "cats-core" % "2.2.0",
  "org.typelevel" %% "cats-effect" % "2.2.0",

  "org.scalatestplus.play" %% "scalatestplus-play" % "5.0.0" % Test
)

// Adds additional packages into Twirl
//TwirlKeys.templateImports += "snorochevskiy.controllers._"

// Adds additional packages into conf/routes
// play.sbt.routes.RoutesKeys.routesImport += "snorochevskiy.binders._"
