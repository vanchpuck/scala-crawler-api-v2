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

//  class PayloadHandler(payload: String) extends HttpHandler {
//    override def handleRequest(exchange: HttpServerExchange): Unit = {
//      exchange.getResponseHeaders.put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8")
//      exchange.setStatusCode(StatusCodes.OK)
//      exchange.getResponseSender.send(payload);
//    }
//  }

//  class RedirectHandler(target: String) extends HttpHandler {
//    override def handleRequest(exchange: HttpServerExchange): Unit = {
//      exchange.getResponseHeaders.put(Headers.LOCATION, target)
//      exchange.setStatusCode(StatusCodes.PERMANENT_REDIRECT)
//    }
//  }

//  object OkHandler {
//    val Payload = "Ok"
//  }

//  class OkHandler(processingTime: Long = 0L) extends HttpHandler {
//    override def handleRequest(exchange: HttpServerExchange): Unit = {
//      Thread.sleep(processingTime)
//      exchange.getResponseHeaders.put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8")
//      exchange.setStatusCode(StatusCodes.OK)
//      exchange.getResponseSender.send(OkHandler.Payload);
//    }
//  }

  private val OkPayload = "Ok"
  private val RobotsTxtPayload =
    """
      |Crawl-delay: 2
      |User-agent: *
      |Disallow: /private/
      |""".stripMargin

//  object RobotsTxtHandler extends HttpHandler {
//    val Payload: String =
//      """
//        |Crawl-delay: 2
//        |User-agent: *
//        |Disallow: /private/
//        |""".stripMargin
//
//    override def handleRequest(exchange: HttpServerExchange): Unit = {
//      exchange.getResponseHeaders.put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8")
//      exchange.setStatusCode(StatusCodes.OK)
//      exchange.getResponseSender.send(Payload);
//    }
//  }
}

class EndToEndSpec extends AnyFlatSpec with BeforeAndAfterAll {

  import izolotov.crawler.util.CrawlQueueHelpers._
  import izolotov.crawler.core.DefaultCrawler._

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
//
//  //  def owerwrite(header: HttpHeader*): Iterable[HttpHeader] => Iterable[HttpHeader] = _ => header
//  def modify(headers: izolotov.crawler.core.HttpHeader*): Iterable[izolotov.crawler.core.HttpHeader] => Iterable[izolotov.crawler.core.HttpHeader] = {
//    val headersMap = headers.groupBy(h => h.name()).map(entry => (entry._1, entry._2.head))
//    defaultHeaders => defaultHeaders.map { h =>
//      headersMap.getOrElse(h.name(), h)
//    }
//  }

  it should "work end-to-end" in {
    // TODO check case user agent sensitivity
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
