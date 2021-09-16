package util.aws

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.dynamodbv2.document.{DynamoDB, Item, KeyAttribute, PrimaryKey, QueryOutcome}
import com.amazonaws.services.dynamodbv2.{AmazonDynamoDB, AmazonDynamoDBClient, AmazonDynamoDBClientBuilder}
import util.config.AwsCred

import scala.jdk.CollectionConverters.ListHasAsScala

class DynamoDbWrapper(awsCred: AwsCred) {

  def delete(tableName: String, columnName: String, value: Any): List[String] =
    withDynamoDb { dynamoDB =>

      val table = dynamoDB.getTable(tableName)
      val tableDescr = table.describe()
      val PrimaryKeyColumns = tableDescr.getKeySchema.asScala.map(_.getAttributeName)

      val itemsToDelete = table.query(columnName, value)
      var deletedItems: List[String] = Nil
      itemsToDelete.forEach{i =>
        val attrs = PrimaryKeyColumns.map(k => new KeyAttribute(k, i.get(k))).toArray
        table.deleteItem(attrs:_*)
        deletedItems = i.toString :: deletedItems
      }
      deletedItems
    }

  def withDynamoDb[A](f: DynamoDB=>A): A = {
    val dynamoDB = new DynamoDB(makeClient())
    try {
      f(dynamoDB)
    } finally {
      dynamoDB.shutdown()
    }
  }

  def withClient[A](f: AmazonDynamoDB=>A): A = {
    val client = makeClient()
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }

  def makeClient(): AmazonDynamoDB = AmazonDynamoDBClient.builder()
    .withRegion(awsCred.region)
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsCred.key, awsCred.secret)))
    .build()
}
