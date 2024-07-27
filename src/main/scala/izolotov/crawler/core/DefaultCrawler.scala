package izolotov.crawler.core

import com.google.common.collect.Lists
import com.google.common.net.HttpHeaders
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRules, SimpleRobotRulesParser}
import org.jsoup.Jsoup
import org.jsoup.nodes.Document

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import scala.jdk.OptionConverters._

object DefaultCrawler {

  private implicit val DefaultHttpClient: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()

  implicit object JsoupDocBodyHandler extends HttpResponse.BodyHandler[Document] {
    override def apply(responseInfo: HttpResponse.ResponseInfo): HttpResponse.BodySubscriber[Document] = {
      val upstream = HttpResponse.BodySubscribers.ofString(StandardCharsets.UTF_8)
      HttpResponse.BodySubscribers.mapping(
        upstream,
        (html: String) => {
          Jsoup.parse(html)
        }
      )
    }
  }

  def doNothingParser(): HttpResponse[Document] => HttpResponse[Document] = {
    val f: HttpResponse[Document] => HttpResponse[Document] = response => response
    f
  }

  implicit val redirectSpotter: HttpResponse[Document] => Option[String] = {
    resp => {
      if (resp.statusCode() >= 300 && resp.statusCode() <= 399) {
        Some(resp.headers().firstValue(HttpHeaders.LOCATION).toScala
          .getOrElse(throw new IllegalStateException("Location header is missing")))
      } else {
        None
      }
    }
  }

  private class DefaultRobotRules(rules: SimpleRobotRules) extends RobotRules {
    override def delay(): Option[Long] = {
      if (rules.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(rules.getCrawlDelay)
    }

    override def allowance(url: URL): Boolean = rules.isAllowed(url.toString)
  }

  implicit val robotsPolicySpotter: HttpResponse[Document] => RobotRules = {
    resp =>
      val rules = new SimpleRobotRulesParser().parseContent(
        resp.uri().toString,
        resp.body().wholeText().getBytes,
        resp.headers().firstValue("Content-Type").orElse("text/plain"),
        // TODO robotName vs userAgent
        Lists.newArrayList(resp.request().headers().firstValue("User-Agent").orElse("").toLowerCase)
      )
      new DefaultRobotRules(rules)
  }

  def httpFetcher[T](
                      httpClient: HttpClient = DefaultHttpClient
                    )(implicit bodyHandler: HttpResponse.BodyHandler[T]): (URL, Iterable[HttpHeader]) => HttpResponse[T] = {
    if (httpClient.followRedirects() != HttpClient.Redirect.NEVER)
      throw new IllegalStateException("Only the NEVER redirect policy allowed")
    (url, headers) =>
      println(f"Fetch: $url")
      val builder = HttpRequest.newBuilder()
        .uri(url.toURI)
      headers.foreach(header => builder.setHeader(header.name(), Option(header.value()).getOrElse("")))
      httpClient.send(builder.build(), bodyHandler)
  }
}
