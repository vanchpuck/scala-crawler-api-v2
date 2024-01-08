package izolotov.crawler

import scala.util.Try

case class Attempt[Doc](url: String, doc: Try[Doc], redirectTarget: Option[String] = None, outLinks: Iterable[String] = Seq())
