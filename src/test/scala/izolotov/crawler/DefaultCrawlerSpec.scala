package izolotov.crawler

import org.eclipse.jetty.http.{HttpHeader, HttpStatus}
import org.eclipse.jetty.server.{Handler, Request, Response, Server}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.eclipse.jetty.server.handler.{ContextHandler, ContextHandlerCollection}
import org.eclipse.jetty.util.Callback
import izolotov.crawler.core.DefaultCrawler._
import DefaultCrawlerSpec._
import com.google.common.net.HttpHeaders
import izolotov.crawler.util.HttpHeaders.Cookie
import org.json4s.native.Json
import org.json4s.DefaultFormats

import java.net.URI
import java.net.http.{HttpClient, HttpRequest}
import java.nio.ByteBuffer

object DefaultCrawlerSpec {
  object RequestHeadersHandler extends Handler.Abstract {
    override def handle(request: Request, response: Response, callback: Callback): Boolean = {
      val payload = Json(DefaultFormats).write(Map(
        HttpHeaders.USER_AGENT -> request.getHeaders.get(HttpHeaders.USER_AGENT).toString,
        HttpHeaders.COOKIE -> request.getHeaders.get(HttpHeaders.COOKIE).toString
      ))
      response.getHeaders.add(HttpHeader.CONTENT_TYPE, "text/plain;charset=utf-8")
      response.write(true, ByteBuffer.wrap(payload.getBytes("utf-8")), callback)
      callback.succeeded()
      true
    }
  }
  object RobotsTxtHandler extends Handler.Abstract {
    val Payload: String =
      """
        |User-agent: *
        |Disallow: /
        |Crawl-delay: 2
        |User-agent: Authorized
        |Allow: /restricted/
        |""".stripMargin
    override def handle(request: Request, response: Response, callback: Callback): Boolean = {
      response.setStatus(HttpStatus.OK_200)
      response.getHeaders.add(HttpHeader.CONTENT_TYPE, "text/plain;charset=utf-8")
      response.write(true, ByteBuffer.wrap(Payload.getBytes("utf-8")), callback)
      callback.succeeded()
      true
    }
  }
  object OkHandler extends Handler.Abstract {
    val Payload: String = "Ok"
    override def handle(request: Request, response: Response, callback: Callback): Boolean = {
      response.setStatus(HttpStatus.OK_200)
      response.getHeaders.add(HttpHeader.CONTENT_TYPE, "text/plain;charset=utf-8")
      response.write(true, ByteBuffer.wrap(Payload.getBytes("utf-8")), callback)
      callback.succeeded()
      true
    }
  }
  object RedirectHandler extends Handler.Abstract {
    val RedirectCom = "http://redirect.com"
    override def handle(request: Request, response: Response, callback: Callback): Boolean = {
      println("call")
      response.setStatus(HttpStatus.PERMANENT_REDIRECT_308)
      response.getHeaders.add(HttpHeader.LOCATION, RedirectCom)
      callback.succeeded()
      true
    }
  }
}

class DefaultCrawlerSpec extends AnyFlatSpec with BeforeAndAfterAll {

  val server = new Server(8080)


  override def beforeAll(): Unit = {
    val redirectContextHandler = new ContextHandler()
    redirectContextHandler.setContextPath("/redirect")
    redirectContextHandler.setHandler(RedirectHandler)

    val okContextHandler = new ContextHandler()
    okContextHandler.setContextPath("/ok")
    okContextHandler.setHandler(OkHandler)

    val robotsContextHandler = new ContextHandler()
    robotsContextHandler.setContextPath("/robots")
    robotsContextHandler.setHandler(RobotsTxtHandler)

    val headersContextHandler = new ContextHandler()
    headersContextHandler.setContextPath("/headers")
    headersContextHandler.setHandler(RequestHeadersHandler)

    val handlerCollection = new ContextHandlerCollection()
    handlerCollection.addHandler(redirectContextHandler)
    handlerCollection.addHandler(okContextHandler)
    handlerCollection.addHandler(robotsContextHandler)
    handlerCollection.addHandler(headersContextHandler)

    server.setHandler(handlerCollection)
    server.start()
  }

  override def afterAll(): Unit = {
    server.stop()
  }

  behavior of "redirectSpotter"

  it should "get the target in case of redirect" in {
    val httpClient: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
    val builder = HttpRequest.newBuilder().uri(new URI(s"http://localhost:${server.getURI.getPort}/redirect/"))
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    assert(redirectSpotter.apply(resp).get == RedirectHandler.RedirectCom)
  }

  it should "get nothing if there is no redirect" in {
    val httpClient: HttpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()
    val builder = HttpRequest.newBuilder().uri(new URI(s"http://localhost:${server.getURI.getPort}/ok/"))
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    assert(redirectSpotter.apply(resp).isEmpty)
  }

  behavior of "robotsPolicySpotter"

  it should "get default rules if there are no user-agent specific rules in robots.txt" in {
    val httpClient: HttpClient = HttpClient.newBuilder().build()
    val builder = HttpRequest.newBuilder()
      .uri(new URI(s"http://localhost:${server.getURI.getPort}/robots/"))
      .header("User-Agent", "stranger")
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    val policy = robotsPolicySpotter.apply(resp)
    assert(policy.delay().get == 2000L)
    assert(!policy.allowance(URI.create(s"http://localhost:${server.getURI.getPort}/restricted/").toURL))
  }

  it should "get user-agent specific rules if they are specified in robots.txt" in {
    val httpClient: HttpClient = HttpClient.newBuilder().build()
    val builder = HttpRequest.newBuilder()
      .uri(new URI(s"http://localhost:${server.getURI.getPort}/robots/"))
      .header("User-Agent", "authorized")
    val resp = httpClient.send(builder.build(), JsoupDocBodyHandler)
    val policy = robotsPolicySpotter.apply(resp)
    assert(policy.delay().isEmpty)
    assert(policy.allowance(URI.create(s"http://localhost:${server.getURI.getPort}/restricted/").toURL))
  }

  behavior of "httpFetcher"

  it should "return correct response for the giving URL" in {
    val resp = httpFetcher().apply(new URI(s"http://localhost:${server.getURI.getPort}/ok/").toURL, Seq(Util.DummyHeader))
    println(resp.toString)
    assert(resp.body().body().text() == OkHandler.Payload)
  }

  it should "send request with headers specified" in {
    implicit val formats: DefaultFormats.type = org.json4s.DefaultFormats
    val cookieKey = "message"
    val cookieValue = "hello"
    val resp = httpFetcher().apply(new URI(s"http://localhost:${server.getURI.getPort}/headers/").toURL, Seq(Cookie(Map(cookieKey -> cookieValue))))
    val headers = Json(DefaultFormats).parse(resp.body().body().text()).extract[Map[String, String]]
    assert(headers(HttpHeaders.COOKIE) == s"$cookieKey=$cookieValue")
  }

}
