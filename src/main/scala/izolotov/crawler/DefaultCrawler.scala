package izolotov.crawler

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
//import izolotov.Sandbox.{Fetched, HostQueue, QueueItem, SeleniumResp}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}

object DefaultCrawler {

  implicit val httpClient = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()

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

//  implicit object JsoupDocFetchedJsonable extends RedirectAnalyzer[HttpResponse[Document]]{
//    val Pattern = ".*url=(.+)\\s*".r
//
//    def analyze(resp: HttpResponse[Document]): Redirectable[HttpResponse[Document]] = {
//      val f: PartialFunction[Element, String] = {
//        case el if el.attributes().asScala.exists(a => a.getKey == "http-equiv" && a.getValue == "refresh") =>
//          el.attributes().get("content")
//      }
//      // TODO other http codes
//      // TODO missing Location header
//      // TODO check headrs cas sensitivity
//      resp.statusCode match {
//        case 302 => Redirect(resp.headers().firstValue("location").get(), resp)
//        case _ => {
//          resp.body()
//            .getElementsByTag("meta")
//            .iterator().asScala
//            .collectFirst(f)
//            .map{
//              content =>
//                val Pattern(url) = content
//                Redirect(url, resp)
//            }
//            .getOrElse(Direct(resp))
//        }
//      }
//    }
//  }

  object HttpFetcher {
    def apply[T]()(implicit bodyHandler: HttpResponse.BodyHandler[T]): HttpFetcher[T] = new HttpFetcher[T](bodyHandler)
  }

  class HttpFetcher[T](bodyHandler: HttpResponse.BodyHandler[T]) {
    def fetch(url: URL)(implicit client: HttpClient): HttpResponse[T] = {
      println(f"Fetch: ${url}")
//      if (url.getHost == "google.com") {
//        throw new Exception("!!!!!!!!!!!!!!")
//      }
      val request = HttpRequest.newBuilder()
        .uri(url.toURI)
        .build()
      val resp = client.send(request, bodyHandler /*BaseHttpFetcher.CustomBodyHandler*/)
//      println(resp)
      resp
    }
  }

//  implicit val managerBuilder = new HostQueueManagerBuilder()

  implicit def host(url: String): URL => Boolean = url => url.getHost == url

  implicit def all(): URL => Boolean = _ => true

  implicit def url(url: String): URL => Boolean = urlObj => urlObj.toString == url

//  ob


//  object FlexibleDelay extends CrawlerOption[FlexibleManagerBuilder, Long] {
//    override def apply(managerBuilder: FlexibleManagerBuilder, value: Long): ManagerBuilder = {
//      managerBuilder.delay(value)
//    }
//  }


}
