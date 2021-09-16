package util.date

import java.time.LocalDateTime

import cats.implicits.catsSyntaxOptionId

object DateUtil {

  def parse8601ToSqlTimestamp(text: String): Option[java.sql.Timestamp] =
    try {
      val localData = LocalDateTime.parse(text, java.time.format.DateTimeFormatter.ISO_DATE_TIME)
      java.sql.Timestamp.valueOf(localData).some
    } catch {
      case _: Exception => None
    }


}
