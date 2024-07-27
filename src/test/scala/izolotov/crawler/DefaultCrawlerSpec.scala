package izolotov.crawler

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import izolotov.crawler.core.DefaultCrawler._
import DefaultCrawlerSpec._
import com.google.common.net.HttpHeaders
import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.util.Headers
import io.undertow.{Handlers, Undertow}
import izolotov.crawler.UndertowUtil.{PayloadHandler, RedirectHandler}
import izolotov.crawler.core.BasicHttpHeader
import org.json4s.native.Json
import org.json4s.DefaultFormats

import java.net.URI
import java.net.http.{HttpClient, HttpRequest}

object DefaultCrawlerSpec {

  private val Port = 8081

  private val RobotsTxtPayload: String =
    """
      |User-agent: *
      |Disallow: /
      |Crawl-delay: 2
      |User-agent: Authorized
      |Allow: /restricted/
      |""".stripMargin

  private val RedirectTarget = "http://redirect.com"
  private val OkPayload = "Ok"

  private val Redirect = new URI(s"http://localhost:$Port/redirect/")
  private val Ok = new URI(s"http://localhost:$Port/ok/")
  private val Robots = new URI(s"http://localhost:$Port/robots/")
  private val Restricted = new URI(s"http://localhost:$Port/restricted/")
  private val Header = new URI(s"http://localhost:$Port/header/")

  object RequestHeadersHandler extends HttpHandler {
    override def handleRequest(exchange: HttpServerExchange): Unit = {
      val payload = Json(DefaultFormats).write(Map(
        HttpHeaders.USER_AGENT -> exchange.getRequestHeaders.get(HttpHeaders.USER_AGENT).getFirst,
        HttpHeaders.COOKIE -> exchange.getRequestHeaders.get(HttpHeaders.COOKIE).getFirst
      ))
      exchange.getResponseHeaders.put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8")
      exchange.getResponseSender.send(payload)
    }
  }
}

class DefaultCrawlerSpec extends AnyFlatSpec with BeforeAndAfterAll {

  val server: Undertow = Undertow.builder()
    .addHttpListener(Port, "localhost")
    .setHandler(
      Handlers.path(Handlers.path())
        .addPrefixPath("/robots", new PayloadHandler(RobotsTxtPayload))
        .addPrefixPath("/redirect", new RedirectHandler(RedirectTarget))
        .addPrefixPath("/ok", new PayloadHandler(OkPayload))
        .addPrefixPath("/header", RequestHeadersHandler)
    ).build()

  override def beforeAll(): Unit = {
    server.start()
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  behavior of "redirectSpotter"

  it should "get the target in case of redirect" in {
    val httpClient: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
    val builder = HttpRequest.newBuilder().uri(Redirect)
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    assert(redirectSpotter.apply(resp).get == RedirectTarget)
  }

  it should "get nothing if there is no redirect" in {
    val httpClient: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
    val builder = HttpRequest.newBuilder().uri(Ok)
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    assert(redirectSpotter.apply(resp).isEmpty)
  }

  behavior of "robotsPolicySpotter"

  it should "get default rules if there are no user-agent specific rules in robots.txt" in {
    val httpClient: HttpClient = HttpClient.newBuilder().build()
    val builder = HttpRequest.newBuilder()
      .uri(Robots)
      .header("User-Agent", "stranger")
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    val policy = robotsPolicySpotter.apply(resp)
    assert(policy.delay().get == 2000L)
    assert(!policy.allowance(Restricted.toURL))
  }

  it should "get user-agent specific rules if they are specified in robots.txt" in {
    val httpClient: HttpClient = HttpClient.newBuilder().build()
    val builder = HttpRequest.newBuilder()
      .uri(Robots)
      .header("User-Agent", "authorized")
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    val policy = robotsPolicySpotter.apply(resp)
    assert(policy.delay().isEmpty)
    assert(policy.allowance(Restricted.toURL))
  }

  behavior of "httpFetcher"

  it should "return correct response for the giving URL" in {
    val resp = httpFetcher().apply(Ok.toURL, Seq(Util.DummyHeader))
    println(resp.toString)
    assert(resp.body().body().text() == OkPayload)
  }

  it should "send request with headers specified" in {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val cookieKey = "message"
    val cookieValue = "hello"
    val resp = httpFetcher().apply(Header.toURL, Seq(BasicHttpHeader(HttpHeaders.COOKIE, s"$cookieKey=$cookieValue")))
    val headers = Json(DefaultFormats).parse(resp.body().body().text()).extract[Map[String, String]]
    assert(headers(HttpHeaders.COOKIE) == s"$cookieKey=$cookieValue")
  }
}
