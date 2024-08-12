package izolotov.htmlunit

import com.google.common.net.HttpHeaders
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRules, SimpleRobotRulesParser}
import izolotov.crawler.core.DefaultCrawler.DefaultRobotRules
import izolotov.crawler.core.{HttpHeader, RobotRules, TheRobotRules}
import org.htmlunit.{DefaultPageCreator, Page, TextPage, WebClient, WebRequest}
//import org.htmlunit.html.HtmlPage

import java.net.{URI, URL}
import java.nio.charset.{Charset, StandardCharsets}
//import java.net.http.HttpRequest
import scala.jdk.CollectionConverters._

object HtmlUnitCrawler {

  private val DefaultWebClient = {
    val client = new WebClient()
    client.getOptions.setRedirectEnabled(false)
    client.getOptions.setThrowExceptionOnFailingStatusCode(false)
    client.getOptions.setThrowExceptionOnScriptError(false)
    client
  }

  def htmlUnitFetcher(webClient: WebClient = DefaultWebClient): (URL, Iterable[HttpHeader]) => Page = {
    if (webClient.getOptions.isRedirectEnabled)
      throw new IllegalStateException("Redirects should be disabled for WebClient")
    (url, headers) =>
      println(f"Fetch: $url")
      val request = new WebRequest(url)
      headers.foreach(h => request.setAdditionalHeader(h.name(), h.value()))
      val page: Page = webClient.getPage(request)
      page
//      val page: Page = webClient.getPage(request)
//      DefaultPageCreator.determinePageType()
//      page
  }

  implicit val redirectSpotter: Page => Option[String] = {
    page => {
      val code = page.getWebResponse.getStatusCode
      if (code >= 300 && code <= 399) {
        println(page.getWebResponse.getResponseHeaders)
        Option(page.getWebResponse.getResponseHeaderValue(HttpHeaders.LOCATION))
          .orElse(throw new IllegalStateException("Location header is missing"))
      } else {
        None
      }
    }
  }

  def doNothingParser(): Page => Page = {
    val f: Page => Page = response => response
    f
  }

  private class DefaultRobotRules(rules: SimpleRobotRules) extends RobotRules {
    override def delay(): Option[Long] = {
      if (rules.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(rules.getCrawlDelay)
    }

    override def allowance(url: URL): Boolean = rules.isAllowed(url.toString)
  }

  implicit val robotsPolicySpotter: Page => RobotRules = {
    page =>
      val rules = new SimpleRobotRulesParser().parseContent(
        page.getUrl.toString,
        page.getWebResponse.getContentAsStream.readAllBytes(),
        Option(page.getWebResponse.getResponseHeaderValue(HttpHeaders.CONTENT_TYPE)).getOrElse("text/plain"),
        // TODO robotName vs userAgent
        Option(page.getWebResponse.getWebRequest.getAdditionalHeader(HttpHeaders.USER_AGENT)).getOrElse("").toLowerCase
      )
      new DefaultRobotRules(rules)
//      TheRobotRules(
//        if (rules.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(rules.getCrawlDelay),
//        url => rules.isAllowed(url.toString)
//      )
//      override def delay(): Option[Long] = {
//        if (rules.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(rules.getCrawlDelay)
//      }
//      override def allowance(url: URL): Boolean = rules.isAllowed(url.toString)
  }

//  private def getDefaultWebClient: WebClient = {
//    val client = new WebClient()
//    client.getOptions.setRedirectEnabled(false)
//    client
//  }

}
