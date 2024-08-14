package izolotov.htmlunit

import com.google.common.net.HttpHeaders
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRules, SimpleRobotRulesParser}
import izolotov.crawler.robots.CommonRobotRules
import izolotov.crawler.core.{HttpHeader, RobotRules}
import org.htmlunit.{Page, TopLevelWindow, WebClient, WebRequest}

import java.net.URL
import java.nio.charset.{Charset, StandardCharsets}
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
    if (webClient.getOptions.isThrowExceptionOnScriptError)
      throw new IllegalStateException("Throwing exception on script error should be disabled WebClient")
    if (webClient.getOptions.isThrowExceptionOnFailingStatusCode)
      throw new IllegalStateException("Throwing exception on failing status code should be disabled WebClient")
    (url, headers) =>
      println(f"Fetch: $url")
      val window = webClient.openWindow(null, url.toString)
//      webClient.getTopLevelWindows
      val request = new WebRequest(url)
      headers.foreach(h => request.setAdditionalHeader(h.name(), h.value()))
      val page: Page = webClient.getPage(window, request)
      page
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
    val f: Page => Page = { page =>
      page.getEnclosingWindow.asInstanceOf[TopLevelWindow].close()
      page
    }
    f
  }

  implicit val robotsPolicySpotter: Page => RobotRules = {
    page =>
      try {
        val rules = new SimpleRobotRulesParser().parseContent(
          page.getUrl.toString,
          page.getWebResponse.getContentAsStream.readAllBytes(),
          Option(page.getWebResponse.getResponseHeaderValue(HttpHeaders.CONTENT_TYPE)).getOrElse("text/plain"),
          // TODO robotName vs userAgent
          Option(page.getWebResponse.getWebRequest.getAdditionalHeader(HttpHeaders.USER_AGENT)).getOrElse("").toLowerCase
        )
        new CommonRobotRules(rules)
      } finally {
        println("Closing !!!!!!!!!!!!!!!")
        page.getEnclosingWindow.asInstanceOf[TopLevelWindow].close()
      }
  }
}
