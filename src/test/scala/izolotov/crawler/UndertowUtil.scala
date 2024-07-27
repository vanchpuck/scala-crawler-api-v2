package izolotov.crawler

import io.undertow.server.{HttpHandler, HttpServerExchange}
import io.undertow.util.{Headers, StatusCodes}
import org.eclipse.jetty.http.{HttpHeader, HttpStatus}
import org.eclipse.jetty.server.{Handler, Request, Response}
import org.eclipse.jetty.util.Callback

import java.nio.ByteBuffer

object UndertowUtil {

  class RedirectHandler(target: String) extends HttpHandler {
    override def handleRequest(exchange: HttpServerExchange): Unit = {
      exchange.getResponseHeaders.put(Headers.LOCATION, target)
      exchange.setStatusCode(StatusCodes.PERMANENT_REDIRECT)
    }
  }

  class PayloadHandler(payload: String, lag: Long = 0L) extends HttpHandler {
    override def handleRequest(exchange: HttpServerExchange): Unit = {
      Thread.sleep(lag)
      exchange.getResponseHeaders.put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8")
      exchange.setStatusCode(StatusCodes.OK)
      exchange.getResponseSender.send(payload)
    }
  }

  object RobotsTxtHandler extends HttpHandler {
    val Payload: String =
      """
        |Crawl-delay: 2
        |User-agent: *
        |Disallow: /private/
        |""".stripMargin

    override def handleRequest(exchange: HttpServerExchange): Unit = {
      exchange.getResponseHeaders.put(Headers.CONTENT_TYPE, "text/plain;charset=utf-8")
      exchange.setStatusCode(StatusCodes.OK)
      exchange.getResponseSender.send(Payload);
    }
  }

  object RobotsTxtHandle1r extends Handler.Abstract {
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
}
