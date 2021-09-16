package util.logs

import java.io.{BufferedReader, StringReader}

import cats.implicits.catsSyntaxEitherId

import scala.util.Using

import util.ops.StringBuilderOps._

sealed trait ParsingRes
case class ParsedStackTrace(st: StackTrace) extends ParsingRes
case class ParsingError(e: Exception) extends ParsingRes
case object NotStackTrace extends ParsingRes

case class StackTrace(frames: List[StackFrame])
case class StackFrame(message: String, calls: List[String])

object StepLogsProcessor {

  // E.g. "20/12/03 21:59:52 INFO "
  val LogEntryHeaderRe = """^\d\d/\d\d/\d\d \d\d:\d\d:\d\d \w+ """.r

  val LogEntryLineRe = """^\d\d/\d\d/\d\d \d\d:\d\d:\d\d \w+ (.*)""".r
  /**
   * Removes useless log entries
   * e.g. 20/12/03 21:59:52 INFO Client: Application report for application_1607029585436_0001 (state: RUNNING)
   *
   * @param reader
   * @return
   */
  def cleanSparkLog(reader: BufferedReader): String = {
    @scala.annotation.tailrec
    def collectText(sb: StringBuilder, r: BufferedReader): StringBuilder = {
      val line = r.readLine()
      if (line eq null) sb
      else if (line.contains("Application report for application_")) collectText(sb, reader)
      else collectText(sb.addLine(line), reader)
    }

    collectText(new StringBuilder, reader).toString()
  }

  /**
   * Takes a EMR Step log file produced by Spark application, and collects stack trace text chucks from it.
   *
   * Note, the first element in the resulting list is actually the last in the original log.
   * @param reader log file buffered reader
   * @return list of stack trace chunks
   */
  def collectStackTraceChunks(reader: BufferedReader): List[String] = {

    @scala.annotation.tailrec
    def collectStackTraces(current: StringBuilder, result: List[String], r: BufferedReader): List[String] = {
      val line = r.readLine()

      line match {
        case null =>
          current ?+ result

        case "" =>
          collectStackTraces(new StringBuilder, current ?+ result, r)

        case LogEntryLineRe(msg) =>
          if (msg.contains("User class threw exception")) collectStackTraces(new StringBuilder().addLine(msg), current ?+ result, r)
          else collectStackTraces(new StringBuilder, current ?+ result, r)

        case l if l.startsWith("Caused by:") =>
          collectStackTraces(current.addLine(l), result, r)

        case l if l.startsWith("Driver stacktrace:") =>
          collectStackTraces(current.addLine(l), result, r)

        case l =>
          if (current.nonEmpty) collectStackTraces(current.addLine(l), result, r)
          else collectStackTraces(current, result, r)
      }
    }

    collectStackTraces(new StringBuilder, Nil: List[String], reader)
  }

  /**
   * Wrapper to for {@link StepLogsProcessor.collectStackTraceChunks(BufferedReader) }
   * @param log
   * @return
   */
  def collectStackTraceChunks(log: String): List[String] = {

    @scala.annotation.tailrec
    def collectStackTraces(current: StringBuilder, result: List[String], r: BufferedReader): List[String] = {
      val line = r.readLine()
      if (line eq null) current ?+ result
      else if (current.nonEmpty) {
        if (line.isEmpty || LogEntryHeaderRe.findFirstIn(line).isDefined) {
          collectStackTraces(new StringBuilder, current ?+ result, r)
        } else collectStackTraces(current.addLine(line), result, r)
      } else if (line.startsWith("Caused by:") || line.startsWith("Driver stacktrace:")) {
        collectStackTraces(current.addLine(line), result, r)
      } else collectStackTraces(current, result, r)
    }

    Using.resource(new BufferedReader(new StringReader(log))) { reader =>
      collectStackTraceChunks(reader)
    }
  }

  val CausedByRe = """Caused by:\s(?<message>.*)""".r
  val UserClassExceptionRe = """^.*User class threw exception: (.*)$""".r
  val CallPointRe = """\sat (?<point>.*)""".r
  val MoreRe = """\t... \d+ more""".r
  val DriverStackTrackRe = """Driver stacktrace:\s(?<message>.*)""".r
  // TODO: add Driver stacktrace

  def parseLastStackTrace(chunks: List[String]): Either[Exception,StackTrace] =
    chunks.view.map(parseStackTrace)
      .filter{
        case NotStackTrace => false
        case _ => true
      }
      .map {
        case ParsedStackTrace(st) => Right(st)
        case ParsingError(e) => Left(e)
      }
      .headOption.getOrElse(Left(new IllegalArgumentException("No stack trace chunks found")))

  def parseStackTrace(text: String): ParsingRes = {

    if (text.eq(null) || text == "") return ParsingError(new IllegalArgumentException("Cannot parse empty text"))

    @scala.annotation.tailrec
    def parseStackTrace(r: BufferedReader, msg: Option[String], calls: List[String], frames: List[StackFrame]): ParsingRes =
      r.readLine() match {
        case null =>
          if (calls.isEmpty) NotStackTrace
          else ParsedStackTrace(StackTrace( (msg.map(StackFrame(_, calls.reverse)) ?:: frames).filter(f=>f.message.nonEmpty && f.calls.nonEmpty).reverse))
        case CausedByRe(message) =>
          parseStackTrace(r, Some(message), Nil, msg.map(StackFrame(_, calls.reverse)) ?:: frames)
        case DriverStackTrackRe(message) =>
          parseStackTrace(r, Some(message), Nil, msg.map(StackFrame(_, calls.reverse)) ?:: frames)
        case UserClassExceptionRe(message) =>
          parseStackTrace(r, Some(message), Nil, msg.map(StackFrame(_, calls.reverse)) ?:: frames)
        case CallPointRe(call) =>
          parseStackTrace(r, msg, call :: calls, frames)
        case MoreRe() =>
          parseStackTrace(r,  msg, calls, frames)
        case line if calls.isEmpty =>
          parseStackTrace(r,  msg.map(_ + "\n" + line), Nil, frames)
        case other =>
          ParsingError(new IllegalArgumentException("Unable to parse: " + other))
      }

    Using.resource(new BufferedReader(new StringReader(text))) {r =>
      parseStackTrace(r, None, Nil, Nil)
    }
  }

  implicit class ListOps[A](val list: List[A]) extends AnyVal {
    def ?::(opA: Option[A]): List[A] = opA match {
      case Some(a) => a :: list
      case None    => list
    }
  }

  implicit class StringBuilderOps(val sb: StringBuilder) extends AnyVal {
    def ?+(list: List[String]): List[String] = {
      if (sb.isEmpty) list
      else sb.toString :: list
    }
  }
}
