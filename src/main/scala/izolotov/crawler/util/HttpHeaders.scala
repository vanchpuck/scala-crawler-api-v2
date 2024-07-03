package izolotov.crawler.util

import izolotov.crawler.core.BasicHttpHeader

object HttpHeaders {
  object Cookie {
    def apply(cookies: Map[String, String]): Cookie = new Cookie(cookies)
  }

  class Cookie(cookies: Map[String, String]) extends BasicHttpHeader(com.google.common.net.HttpHeaders.COOKIE, cookies.map(entry => s"${entry._1}=${entry._2}").mkString("; ")) {
    override def hashCode(): Int = super.hashCode()

    override def equals(obj: Any): Boolean = super.equals(obj)
  }
}
