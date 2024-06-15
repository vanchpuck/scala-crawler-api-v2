package izolotov.crawler

import com.google.common.collect.Lists
import com.google.common.util.concurrent.ThreadFactoryBuilder
import crawlercommons.robots.{BaseRobotRules, SimpleRobotRules, SimpleRobotRulesParser}
import izolotov.CrawlingQueue
import org.jsoup.nodes.Document

import java.net.URL
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.{Failure, Success, Try}

object SuperNewCrawlerApi {

  private val DaemonThreadFactory = new ThreadFactoryBuilder().setDaemon(true).build

  object NewCrawler {
    def read(urls: CrawlingQueue): ExtractionBuilder = {
      new ExtractionBuilder(urls)
    }

  }

  val DefaultDelay = 0L
  val DefaultRedirectDepth = 1
  val DefaultTimeout = 10000L
  val DoNotFollowRedirectPolicy: URL => Int = _ => 0
  val RespectRobotsTxtPolicy = true
  val AllowAllPolicy: URL => Boolean = _ => true

  case class GlobalConf[Raw](
                              parallelism: Int,
                              redirect: Raw => Option[URL]
                            )

  case class UrlConf[Raw, Doc](
                                fetcher: URL => Raw,
                                parser: Raw => Doc,
                                delay: Long,
                                redirectPattern: URL => Boolean,
                                redirectDepth: Int
                              )

  case class Configuration[Raw, Doc] (
                                       parallelism: Int,
                                       redirect: Raw => Option[String],
                                       robotsHandler: Raw => RobotRules,
                                       queueLength: Int,
                                       allowancePolicy: URL => Boolean,
                                       fetcher: URL => Raw,
                                       parser: URL => Raw => Doc,
                                       delay: URL => Long,
                                       timeout: URL => Long,
                                       redirectPolicy: URL => URL => Int,
                                       robotsTxtPolicy: URL => Boolean
                                     )


  
  // TODO refactor to class (not a case class)
  case class ConfigurationBuilder[Raw, Doc] (
                                              parallelism: Int,
                                              redirect: Raw => Option[String],
                                              robotsHandler: Raw => RobotRules,
                                              fetcher: URL => Raw,
                                              parser: Raw => Doc,
                                              allowancePolicy: URL => Boolean = AllowAllPolicy,
                                              queueLength: Int = Int.MaxValue,
                                              delay: Long = DefaultDelay,
                                              timeout: Long = DefaultTimeout,
                                              redirectPolicy: URL => Int = DoNotFollowRedirectPolicy,
                                              robotsTxtPolicy: Boolean = RespectRobotsTxtPolicy
                                            ) {
    private var getFetcher: PartialFunction[URL, Raw] = {case url if true => this.fetcher(url)}
    private var getParser: PartialFunction[URL, Raw => Doc] = {case _ if true => this.parser}
    private var getDelay: PartialFunction[URL, Long] = {case _ if true => this.delay}
    private var getTimeout: PartialFunction[URL, Long] = {case _ if true => this.timeout}
    private var getRedirectPolicy: PartialFunction[URL, URL => Int] = {case _ if true => this.redirectPolicy}
    private var getRobotsTxtPolicy: PartialFunction[URL, Boolean] = {case _ if true => this.robotsTxtPolicy}

    def build(): Configuration[Raw, Doc] = {
      Configuration[Raw, Doc] (
        parallelism,
        redirect,
        robotsHandler,
        queueLength,
        allowancePolicy,
        getFetcher,
        getParser,
        getDelay,
        getTimeout,
        getRedirectPolicy,
        getRobotsTxtPolicy
      )
    }

    def addConf (
                  predicate: URL => Boolean,
                  fetcher: URL => Raw = this.fetcher,
                  parser: Raw => Doc = this.parser,
                  delay: Long = this.delay,
                  timeout: Long = this.timeout,
                  redirectPolicy: URL => Int = this.redirectPolicy,
                  robotsTxtPolicy: Boolean = this.robotsTxtPolicy
                ): ConfigurationBuilder[Raw, Doc] = {
      // FIXME
      val fnRedirectPolicy: PartialFunction[URL, URL => Int] = {case url if predicate(url) => redirectPolicy}
      //      val fnRobotsTxtPolicy: PartialFunction[URL, Boolean] = {case url if predicate(url) => robotsTxtPolicy(url)}
      val fnFetcher: PartialFunction[URL, Raw] = {case url if predicate(url) => fetcher(url)}
      if (fetcher != this.fetcher) getFetcher = fnFetcher.orElse(getFetcher)
      if (parser != this.parser) getParser = toPF(predicate, parser).orElse(getParser)
      if (delay != this.delay) getDelay = toPF(predicate, delay).orElse(getDelay)
      if (robotsTxtPolicy != this.robotsTxtPolicy) getRobotsTxtPolicy = toPF(predicate, robotsTxtPolicy).orElse(getRobotsTxtPolicy)
//      if (allowancePolicy != this.allowancePolicy) getAllowancePolicy = toPF(predicate, allowancePolicy).orElse(getAllowancePolicy)
      if (timeout != this.timeout) getTimeout = toPF(predicate, timeout).orElse(getTimeout)
      if (redirectPolicy != this.redirectPolicy) getRedirectPolicy =
        fnRedirectPolicy.orElse(getRedirectPolicy)
      this
      //      if (robotsTxtPolicy != this.robotsTxtPolicy) getRobotsTxtPolicy =
      //        fnRobotsTxtPolicy.orElse(getRobotsTxtPolicy)
    }

    private def toPF[A](predicate: URL => Boolean, fn: A): PartialFunction[URL, A] = {
      val pf: PartialFunction[URL, A] = {case url if predicate(url) => fn}
      pf
    }
  }

  case class ConfBuilder[Raw, Doc](
                                  global: GlobalConf[Raw],
                                  default: UrlConf[Raw, Doc]
                             ) {
    var getConf: PartialFunction[URL, UrlConf[Raw, Doc]] = {case _ if true => default}
    def addConf(predicate: URL => Boolean, conf: UrlConf[Raw, Doc]): ConfBuilder[Raw, Doc] = {
      val pf: PartialFunction[URL, UrlConf[Raw, Doc]] = {case url if predicate(url) => conf}
      getConf = pf.orElse(getConf)
      this
    }
    def conf(url: URL): UrlConf[Raw, Doc] = {
      getConf(url)
    }
  }

//  object RobotsRulesExtractor {
//    // TODO specify user agent
//    def extract(robotsTxtURL: URL): RobotRules = {
//      val client = HttpClient.newHttpClient();
//      val request = HttpRequest.newBuilder()
//        .uri(robotsTxtURL.toURI)
//        .build();
//      val response = client.send(request, HttpResponse.BodyHandlers.ofString())
//      val raw = new SimpleRobotRulesParser().parseContent(
//        robotsTxtURL.toString,
//        response.body().getBytes,
//        response.headers().firstValue("Content-Type").orElse("text/plain"),
//        Lists.newArrayList("bot")
//      )
//      RobotsRules(delay = if (raw.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(raw.getCrawlDelay))
//    }
//  }

//  object ExtractionBuilder {
//    val DefaultRedirectPolicy: URL => Int = _ => 0
//  }

//  sealed trait Attmpt[Doc]

  class ExtractionBuilder(urls: CrawlingQueue) {
    def extractWith[Raw, Doc](
                               fetcher: URL => Raw,
                               parser: Raw => Doc,
                               parallelism: Int = 10,
                               queueLength: Int = Int.MaxValue,
                               allowancePolicy: URL => Boolean = _ => true,
                               delay: Long = 0L,
                               timeout: Long = 30000L,
                               redirectPolicy: URL => Int = _ => 0,
                               robotsTxtPolicy: Boolean = true
                             )(implicit redirect: Raw => Option[String], robots: Raw => RobotRules): BranchPredicateBuilder[Raw, Doc] = {
      val confBuilder = ConfigurationBuilder(
        parallelism,
        redirect,
        robots,
        fetcher,
        parser,
        allowancePolicy,
        queueLength,
        delay,
        timeout,
        redirectPolicy,
        robotsTxtPolicy
      )
      new BranchPredicateBuilder[Raw, Doc](
        urls, confBuilder
      )
    }
  }

  class BranchPredicateBuilder[Raw, Doc](
                                          urls: CrawlingQueue,
                                          builder: ConfigurationBuilder[Raw, Doc]
                                        ) {
    // FIXME get rid of global
//    implicit val ec = ExecutionContext.global
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
      Executors.newSingleThreadExecutor(DaemonThreadFactory)
    )
//    implicit val ec: ExecutionContext = ExecutionContext.global

    def foreach[Err](onSuccess: Doc => Unit, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
      foreachRaw(res => res.doc)(doc => doc.map(onSuccess).recover{case e if true => onErr(e)})
    }

    def foreach(fn: Attempt[Doc] => Unit): Unit = {
      foreachRaw(res => res)(fn)
    }

    private def foreachRaw[A](get: Attempt[Doc] => A)(fn: A => Unit): Unit = {
      val core = new CrawlCore[Raw, Doc](urls, builder.build())
      core.foreach(get.andThen(fn))
    }

    def when(predicate: URL => Boolean): BranchConfigurationBuilder[Raw, Doc] = {
      new BranchConfigurationBuilder[Raw, Doc](urls, builder, predicate)
    }
  }

  class BranchConfigurationBuilder[Raw, Doc](urls: CrawlingQueue, conf: ConfigurationBuilder[Raw, Doc], predicate: URL => Boolean) {
    def set(
             fetcher: URL => Raw = conf.fetcher,
             parser: Raw => Doc = conf.parser,
             delay: Long = conf.delay,
             timeout: Long = conf.timeout,
             redirectPolicy: URL => Int = conf.redirectPolicy,
             robotsTxtPolicy: Boolean = conf.robotsTxtPolicy
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, fetcher, parser, delay, timeout, redirectPolicy, robotsTxtPolicy)
      new BranchPredicateBuilder[Raw, Doc](urls, conf)
    }
  }

  class JsoupHttpFetcher

  import izolotov.crawler.DefaultCrawler._
//  implicit val httpFetcher: Function[URL, HttpResponse[Document]] = HttpFetcher().fetch
  implicit val httpFetcher = DefaultHttpFetcher(HttpClient.newBuilder().build())
  implicit val defaultParser: HttpResponse[Document] => String = resp => "resp from: " + resp.uri().toString
//  implicit val anotherParser: HttpResponse[Document] => String = resp => "body: " + resp.body().html().substring(0, 10)
  implicit val redirectExtractor: HttpResponse[Document] => Option[String] = {
    resp => {
      if (resp.uri().toString == "http://example.com")
        Some("http://redirect.com")
      else
        None
    }
  }
  class DefaultRobotRules(rules: SimpleRobotRules) extends RobotRules {
    override def delay(): Option[Long] = {
      if (rules.getCrawlDelay == BaseRobotRules.UNSET_CRAWL_DELAY) None else Some(rules.getCrawlDelay)
    }

    override def allowance(url: URL): Boolean = rules.isAllowed(url.toString)
  }

  implicit val robotsPolicyExtractor: HttpResponse[Document] => RobotRules = {
    resp =>
      val rules = new SimpleRobotRulesParser().parseContent(
        resp.uri().toString,
        resp.body().wholeText().getBytes,
        resp.headers().firstValue("Content-Type").orElse("text/plain"),
        // TODO robotName vs userAgent
        Lists.newArrayList(resp.request().headers().firstValue("User-Agent").orElse(""))
      )
      new DefaultRobotRules(rules)
  }

//  def respect(): URL => RobotRules = url => RobotsRulesExtractor.extract(url)
//  def ignore(): URL => RobotRules = _ => RobotRules(delay = None)

  def all(depth: Int = 5): URL => Int = _ => depth
  def no(): URL => Int = _ => 0
  object Redirect {
    def url(url: String, depth: Int = 5): URL => Int = _url => if (_url.toString == url) depth else 0
  }
  object Allow {
    def url(url: String): URL => Boolean = _url => _url.toString == url
  }

  object DefaultHttpFetcher {
    def apply[T](httpClient: HttpClient)(implicit bodyHandler: HttpResponse.BodyHandler[T]): DefaultHttpFetcher[T] = new DefaultHttpFetcher[T](httpClient)
  }

  class DefaultHttpFetcher[T](httpClient: HttpClient)(implicit bodyHandler: HttpResponse.BodyHandler[T]) {
    if (httpClient.followRedirects() != HttpClient.Redirect.NEVER)
      throw new IllegalStateException("Only the NEVER redirect policy allowed")
    // TODO add headers
    def fetcher(userAgent: String = null, cookies: Map[String, String] = Map.empty): URL => HttpResponse[T] = {
      url =>
        println(f"Fetch: ${url}")
        val builder = HttpRequest.newBuilder()
          .uri(url.toURI)
        if (userAgent != null) builder.setHeader("User-Agent", userAgent)
        builder.setHeader("Cookie", cookies.map(entry => s"${entry._1}=${entry._2}").mkString("; "))
        httpClient.send(builder.build(), bodyHandler)
    }
  }

  val trekkinnParser: HttpResponse[Document] => String = {
    resp =>
//      val doc = Jsoup.parse(inStream, charset.name(), urlString)
      val priceStr = resp.body().select("p#total_dinamic").first().text()
      priceStr.replaceAll("[^0-9,.]", "")
  }// resp => "resp from: " + resp.uri().toString

  def main(args: Array[String]): Unit = {
    val userAgent = "Mozilla/5.0 (platform; rv:geckoversion) Gecko/geckotrail Firefox/firefoxversion"
    NewCrawler
      .read(new CrawlingQueue(Seq(
        "http://sdf:sds:sf",
        "http://example.com",
        "http://example.com",
        "http://example.com",
        "http://example.com",
        "httm",
//        "https://www.densurka.ru/",
//        "https://www.densurka.ru/",
//        "https://www.tradeinn.com/trekkinn/ru/petzl-nomic-%D0%9B%D0%B5%D0%B4%D0%BE%D1%80%D1%83%D0%B1/137053842/p"
      )))
      .extractWith(
        fetcher = httpFetcher.fetcher(userAgent, cookies = Map("id_pais" -> "75")),
        parser = defaultParser,
        timeout = 10000L,
        redirectPolicy = no(),
        allowancePolicy = Allow.url("http://example.com")
      )
      .when(url("http://example.com")).set(delay = 500L, redirectPolicy = Redirect.url("http://redirect.com"))
//      .when(url("https://www.tradeinn.com/trekkinn/ru/petzl-nomic-%D0%9B%D0%B5%D0%B4%D0%BE%D1%80%D1%83%D0%B1/137053842/p")).set(parser = trekkinnParser)
//      .when(url("https://www.densurka.ru")).set(delay = 3000L, robotsTxtPolicy = true)
      .foreach(println, println)//err => err.printStackTrace())
  }


  // TODO:
  // 3. Allow/disallow (allowance policy)


}
