package izolotov.htmlunit

import izolotov.crawler.core.Api.Crawler
import izolotov.crawler.core.Api.Util._
import org.htmlunit.DefaultPageCreator
//import izolotov.crawler.core.DefaultCrawler._
import izolotov.htmlunit.HtmlUnitCrawler._
import org.htmlunit.html.HtmlPage
import org.htmlunit.{Page, WebClient, WebRequest}

import java.net.URI

//import org.h

object Tester {
  import izolotov.crawler.util.CrawlQueueHelpers._
  def main(args: Array[String]): Unit = {
    Crawler
      .read(Seq("http://www.bbc.com"))
      .extract(
        fetcher = htmlUnitFetcher(),
        parser = doNothingParser(),
        redirectPolicy = follow.all()
      )
      .foreach(println, println)
  }
//  DefaultPageCreator.
}
