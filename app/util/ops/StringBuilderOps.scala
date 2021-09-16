package util.ops

object StringBuilderOps {

  implicit class RichStringBuilder(val v: StringBuilder) extends AnyVal {
    def addLine(s: String): StringBuilder = v.append(s).append("\n")
  }

}
