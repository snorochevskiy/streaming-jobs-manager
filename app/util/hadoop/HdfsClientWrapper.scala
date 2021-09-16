package util.hadoop

import java.net.URI
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation

/**
 * Custom wrapper for HDFS java client, that uses direct connection, not WebHDFS service that is separate
 * HTTP based API for interaction with HDFS.
 *
 * To get the info about HDFS address on a EMR instance, login via SSH and execute: hdfs getconf -confKey fs.defaultFS
 *
 * Note: EMR by default also runs WebHDFS that is accessible on http://EMR_MASTER:9870/webhdfs/v1/xxx
 *
 * @param uri a URI for an instance that runs HDFS
 *            For Amazon EMR it is usually like hdfs://96.113.30.133:8020
 *            Note that 8020 is a common port for direct HDFS service access, NOT WebHDFS.
 */
class HdfsClientWrapper(uri: String) {

  /**
   * Deletes given folder
   * @param pathToDelete full directory path starting from /
   */
  def deleteIfExists(pathToDelete: String): Boolean =
    withHdfs { hdfs =>
      val hdfsPath = new Path(pathToDelete)
      if (hdfs.exists(hdfsPath)) hdfs.delete(hdfsPath, true)
      else false
    }

  def withHdfs[A](f: FileSystem=>A): A = {
    val ugi = UserGroupInformation.createRemoteUser("hadoop")
    ugi.doAs(new PrivilegedExceptionAction[A]() {
      override def run(): A = {
        val configuration = new Configuration()
        configuration.set("hadoop.job.ugi", "hadoop")

        val hdfs: FileSystem = FileSystem.get(new URI(uri), configuration)

        try {
          f(hdfs)
        } finally {
          hdfs.close()
        }
      }
    })
  }
}

object HdfsClientWrapper {
  def fromUri(uri: String) = new HdfsClientWrapper(uri)

  def apply(ip: String, port: Int = 8020) = new HdfsClientWrapper(s"hdfs://$ip:$port")
}

