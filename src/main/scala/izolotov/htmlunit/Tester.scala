package izolotov.htmlunit

import izolotov.crawler.core.Api.Crawler
import izolotov.crawler.core.Api.Util._
import izolotov.crawler.core.Attempt
import izolotov.htmlunit.HtmlUnitCrawler._
import org.htmlunit.Page

object Tester {
  import izolotov.crawler.util.CrawlQueueHelpers._
  def main(args: Array[String]): Unit = {
    val fn: Attempt[Page] => Unit = a => println("Out: " + a.url + " " + a)
    Crawler
      .read(Seq("https://arcteryx.com/de/en", "http://www.bbc.com", "http://example.com"))
      .extract(
        fetcher = htmlUnitFetcher(),
        parser = HtmlUnitCrawler.doNothingParser(),
        redirectPolicy = follow.all()
      )
      .foreach(fn)
  }
}
