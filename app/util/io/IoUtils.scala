package util.io

import java.io.{Closeable, InputStream}

object IoUtils {

  implicit class CloseableOps[A <: Closeable](val self: A) extends AnyVal {
    def useAndClose[B](f: A=>B): B =
      try { f(self) }
      finally { self.close() }
  }

}
