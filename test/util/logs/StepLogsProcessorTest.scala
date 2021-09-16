package util.logs

import java.io.{BufferedReader, InputStreamReader, StringReader}

import org.scalatest.{MustMatchers, WordSpec}

import scala.io.Source
import scala.util.Using

class StepLogsProcessorTest extends WordSpec with MustMatchers {

  "StackTraceParser" must {
    "clean log" in {
      val testLog =
        """
          |20/11/18 20:26:40 INFO SomeClass: Useful entry
          |20/11/18 20:26:41 INFO Client: Application report for application_1605214884276_0014 (state: RUNNING)
          |20/11/18 20:26:42 INFO SomeClass: Another useful entry
          |""".stripMargin

      val result = Using.resource(new BufferedReader(new StringReader(testLog))) {reader =>
        StepLogsProcessor.cleanSparkLog(reader)
      }

      result mustBe
        """
          |20/11/18 20:26:40 INFO SomeClass: Useful entry
          |20/11/18 20:26:42 INFO SomeClass: Another useful entry
          |""".stripMargin
    }

    "cut out stack traces from a log" in {

      val log = Using.resource(Source.fromFile("test/resources/log-parser/missing_block_error.txt")) { src =>
        src.mkString
      }

      val res = StepLogsProcessor.collectStackTraceChunks(log)

      res.length mustBe 2

      res(0).startsWith("Driver stacktrace:") mustBe true
      res(0).contains("Could not obtain block: BP-183115326-96.113.30.153-1605540097659:blk_1073969993_240365") mustBe true

      res(1).startsWith("Caused by: org.apache.spark.SparkException: Writing job aborted.") mustBe true
      res(1).contains("Could not obtain block: BP-183115326-96.113.30.153-1605540097659:blk_1073969993_240365") mustBe true
      println(res)
    }

    "parse stack trace chunk" in {
      val st =
        """Caused by: org.apache.spark.SparkException: Writing job aborted.
          |	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2(WriteToDataSourceV2Exec.scala:413)
          |	at org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec.writeWithV2$(WriteToDataSourceV2Exec.scala:361)
          |	... 1 more
          |Caused by: org.apache.spark.SparkException: due to executor 12): org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-183115326
          |	at org.apache.hadoop.hdfs.DFSInputStream.refetchLocations(DFSInputStream.java:879)
          |	at org.apache.hadoop.hdfs.DFSInputStream.chooseDataNode(DFSInputStream.java:862)
          |	at org.apache.hadoop.hdfs.DFSInputStream.chooseDataNode(DFSInputStream.java:841)
          |	at org.apache.hadoop.hdfs.DFSInputStream.blockSeekTo(DFSInputStream.java:567)""".stripMargin

      val ParsedStackTrace(stackTrace) = StepLogsProcessor.parseStackTrace(st)

      stackTrace.frames.length mustBe 2
      stackTrace.frames(1).message must include ("org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block")
    }

    "provide with fail root cause" in {

      val cleanedLog = Using.resource{
        new BufferedReader(
          new InputStreamReader(
            this.getClass.getClassLoader.getResourceAsStream("log-parser/missing_block_error2.txt")
          )
        )
      } {reader =>
        StepLogsProcessor.cleanSparkLog(reader)
      }

      val stackTraces = StepLogsProcessor.collectStackTraceChunks(cleanedLog)
      val lastStackTrace = StepLogsProcessor.parseLastStackTrace(stackTraces)
      val cause = lastStackTrace.getOrElse(null).frames.last.message

      cause mustBe "org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-497959199-96.113.30.135-1607028926403:blk_1073795726_55426 file=/snmp-checkpoints/offsets/128"
    }

    "BlockMissingException in sample 3" in {
      val cleanedLog = Using.resource{ new BufferedReader(new InputStreamReader(
        this.getClass.getClassLoader.getResourceAsStream("log-parser/missing_block_error3.txt")))
      } (StepLogsProcessor.cleanSparkLog)

      val stackTraces = StepLogsProcessor.collectStackTraceChunks(cleanedLog)
      val lastStackTrace = StepLogsProcessor.parseLastStackTrace(stackTraces)
      val cause = lastStackTrace.getOrElse(null).frames.last.message

      cause mustBe "org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-735645629-96.113.30.100-1616274425646:blk_1073825893_85207 file=/streaming-jobs-checkpoints/state/0/111/319.snapshot"
    }

    "BlockMissingException in sample 4" in {
      val cleanedLog = Using.resource{ new BufferedReader(new InputStreamReader(
        this.getClass.getClassLoader.getResourceAsStream("log-parser/missing_block_error4.txt")))
      } (StepLogsProcessor.cleanSparkLog)

      val stackTraces = StepLogsProcessor.collectStackTraceChunks(cleanedLog)
      val lastStackTrace = StepLogsProcessor.parseLastStackTrace(stackTraces)
      val cause = lastStackTrace.getOrElse(null).frames.last.message

      cause mustBe "org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-735645629-96.113.30.100-1616274425646:blk_1073825835_85149 file=/streaming-jobs-checkpoints/state/0/19/319.snapshot"
    }

    "BlockMissingException in sample 5" in {
      val cleanedLog = Using.resource{ new BufferedReader(new InputStreamReader(
        this.getClass.getClassLoader.getResourceAsStream("log-parser/missing_block_error5.txt")))
      } (StepLogsProcessor.cleanSparkLog)

      val stackTraces = StepLogsProcessor.collectStackTraceChunks(cleanedLog)
      val lastStackTrace = StepLogsProcessor.parseLastStackTrace(stackTraces)
      val cause = lastStackTrace.getOrElse(null).frames.last.message

      cause mustBe "org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-1947480113-96.113.30.160-1615924351536:blk_1074219177_521118 file=/streaming-jobs-checkpoints/metadata"
    }

    "BlockMissingException in sample 6" in {
      val cleanedLog = Using.resource{ new BufferedReader(new InputStreamReader(
        this.getClass.getClassLoader.getResourceAsStream("log-parser/missing_block_error6.txt")))
      } (StepLogsProcessor.cleanSparkLog)

      val stackTraces = StepLogsProcessor.collectStackTraceChunks(cleanedLog)
      val lastStackTrace = StepLogsProcessor.parseLastStackTrace(stackTraces)
      val cause = lastStackTrace.getOrElse(null).frames.last.message

      cause mustBe "org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-1947480113-96.113.30.160-1615924351536:blk_1075315063_1786033 file=/streaming-jobs-checkpoints/offsets/95"
    }
  }
}
