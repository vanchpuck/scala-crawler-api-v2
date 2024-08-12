package izolotov.htmlunit

import izolotov.htmlunit.HtmlUnitCrawler._

import java.net.URI

object FetcherRunner {
  def main(args: Array[String]): Unit = {
    val res = htmlUnitFetcher().apply(URI.create("http://www.bbc.com/").toURL, Iterable.empty)
    println(res.getWebResponse.getContentAsString)
  }
}
