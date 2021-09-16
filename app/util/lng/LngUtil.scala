package util.lng

import play.api.Logger

object LngUtil {

  implicit class EitherOps[A](val self: Either[Throwable,A]) extends AnyVal {
    def toOptWithLog()(implicit logger: Logger): Option[A] = self match {
      case Left(err) =>
        logger.warn("Unable to unwrap Option", err)
        None
      case Right(v) =>
        Some(v)
    }
  }

}
