package util.aws

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.jar.JarInputStream
import java.util.zip.GZIPInputStream

import cats.effect.Resource
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsRequest, ListObjectsV2Request, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3Client, AmazonS3ClientBuilder, AmazonS3URI}
import util.config.AwsCred

import scala.jdk.CollectionConverters._
import scala.util.{Try, Using}

class S3Client(awsCred: AwsCred) {

  def s3Client() = AmazonS3Client.builder()
    .withRegion(awsCred.region)
    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsCred.key, awsCred.secret)))
    .build()

  def withClient[A](f: AmazonS3=>A): A = {
    val client = s3Client()
    try {
      f(client)
    } finally {
      client.shutdown()
    }
  }

  def delete(bucket: String, path: String): Unit =
    withClient { s3 =>
      s3.deleteObject(bucket, path)
    }

  def listFiles(bucket: String, path: String): List[S3ObjectSummary] =
    withClient { s3 =>
      val req = new ListObjectsV2Request().withBucketName(bucket).withPrefix(path)
      val resp = s3.listObjectsV2(req)
      resp.getObjectSummaries().asScala.toList
    }

  def readJarStream[A](bucket: String, key: String)(f: JarInputStream=>A): Either[Throwable,A] =
    withClient { client =>
      Try {
        val s3Object = client.getObject(bucket, key)
        Using.resource(new JarInputStream(s3Object.getObjectContent))(f)
      }.toEither
    }

  def readAsGZippedTextStream[A](bucket: String, key: String)(f: BufferedReader=>Either[Exception,A]): Either[Exception,A] =
    withClient { client =>
      val s3Object = client.getObject(bucket, key)
      val reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(s3Object.getObjectContent)))
      try {
        f(reader)
      } catch {
        case e: Exception => Left(e)
      } finally {
        reader.close()
      }
    }

}
