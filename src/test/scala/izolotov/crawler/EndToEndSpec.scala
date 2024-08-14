package izolotov.crawler

import com.google.common.net.HttpHeaders
import io.undertow.{Handlers, Undertow}
import izolotov.crawler.EndToEndSpec._
import izolotov.crawler.UndertowUtil.{PayloadHandler, RedirectHandler}
import izolotov.crawler.core.BasicHttpHeader
import izolotov.crawler.core.Api.Util._
import izolotov.crawler.core.Api.Crawler
import izolotov.crawler.core.Attempt
import izolotov.crawler.core.ParallelExtractor.{NotAllowedByRobotsTxtException, NotAllowedByUserException, TimeoutException}
import org.jsoup.nodes.Document
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable
import java.net.http.HttpResponse
import scala.util.Try

object EndToEndSpec {
  val Port: Int = 8081

  val Host1 = "localhost"
  val Host2 = "127.0.0.1"
  val Host1Private = s"http://$Host1:$Port/private/"
  val Host2Private = s"http://$Host2:$Port/private/"
  val Host1Disallowed = s"http://$Host1:$Port/disallowed/"
  val Host1Timeout = s"http://$Host1:$Port/timeout/"
  val Host1Redirect = s"http://$Host1:$Port/redirect"
  val Host2Redirect = s"http://$Host2:$Port/redirect"
  val Host1RedirectTarget = s"http://$Host1:$Port/target"

  private val OkPayload = "Ok"
  private val RobotsTxtPayload =
    """
      |Crawl-delay: 2
      |User-agent: *
      |Disallow: /private/
      |""".stripMargin
}

class EndToEndSpec extends AnyFlatSpec with BeforeAndAfterAll {

  import izolotov.crawler.util.CrawlQueueHelpers._
  import izolotov.crawler.http.HttpCrawler._

  behavior of "Crawler"

  val server: Undertow = Undertow.builder()
    .addHttpListener(Port, "localhost")
    .setHandler(
      Handlers.path(Handlers.path())
        .addPrefixPath("/robots.txt", new PayloadHandler(RobotsTxtPayload))
        .addPrefixPath("/", new PayloadHandler(OkPayload))
        .addPrefixPath("/timeout", new PayloadHandler(OkPayload, 1100L))
        .addPrefixPath("/redirect", new RedirectHandler(Host1RedirectTarget))
    ).build()

  override def beforeAll(): Unit = {
    server.start();
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  it should "work end-to-end" in {
    val out: mutable.Map[String, Try[HttpResponse[Document]]] = mutable.Map()
    val addToOut: Attempt[HttpResponse[Document]] => Unit = att => out(att.url) = att.doc
    val urls = Seq(
      Host1Private,
      Host2Private,
      Host1Disallowed,
      Host1Timeout,
      Host1Redirect
    )
    Crawler.read(urls).extract(
      fetcher = httpFetcher(),
      parser = doNothingParser(),
      headers = Seq(BasicHttpHeader(HttpHeaders.USER_AGENT, "Regular")),
      allowancePolicy = disallow.url(Host1Disallowed),
      timeout = 1000L,
      delay = 10L,
      redirectPolicy = follow.all()
    ).when(host(Host1)).set(
      respectRobotsTxt = false,
      headers = modify(BasicHttpHeader(HttpHeaders.USER_AGENT, "Authorized")),
      redirectPolicy = prevent.url(Host1RedirectTarget)
    ).foreach(addToOut)
    assert(out(Host1Private).get.body().text() == OkPayload)
    assertThrows[NotAllowedByRobotsTxtException](out(Host2Private).get)
    assertThrows[NotAllowedByUserException](out(Host1Disallowed).get)
    assertThrows[TimeoutException](out(Host1Timeout).get)
    assert(!out.contains(Host1RedirectTarget))
  }
}
