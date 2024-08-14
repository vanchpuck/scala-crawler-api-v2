package izolotov.htmlunit

import izolotov.htmlunit.HtmlUnitCrawler._
import org.htmlunit.{Page, WebClient, WebRequest}

import java.net.URI

object FetcherRunner {
  def main(args: Array[String]): Unit = {
    val client = new WebClient()
    client.getOptions.setRedirectEnabled(false)
    client.getOptions.setThrowExceptionOnFailingStatusCode(false)
    client.getOptions.setThrowExceptionOnScriptError(false)
    val res = htmlUnitFetcher().apply(URI.create("http://www.bbc.com/").toURL, Iterable.empty)
    println(res.getWebResponse.getContentAsString)
    val w1 = client.openWindow(null, "1")
    val r1 = new WebRequest(URI.create("http://bbc.com").toURL)
    val p1: Page = client.getPage(w1, r1)
    println(p1.getWebResponse.getContentAsString)
    println(w1.isClosed)
    val r2 = new WebRequest(URI.create("http://arcterys.com").toURL)
    val p2: Page = client.getPage(r2)
    println(p2)
    println(w1.isClosed)
  }
}
