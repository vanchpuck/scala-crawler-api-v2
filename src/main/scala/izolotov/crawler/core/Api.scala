package izolotov.crawler.core

import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.net.URL
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Api {

  object Util {
    val DefaultDepth: Int = 5

    def all(): URL => Boolean = _ => true

    def host(host: String): URL => Boolean = url => url.getHost.toLowerCase == host.toLowerCase

    def url(url: String): URL => Boolean = _url => _url.toString.toLowerCase == url.toLowerCase

    def modify(headers: izolotov.crawler.core.HttpHeader*): Iterable[izolotov.crawler.core.HttpHeader] => Iterable[izolotov.crawler.core.HttpHeader] = {
      baseHeaders => {
        val baseHeadersMap = baseHeaders.groupBy(h => h.name()).map(entry => (entry._1, entry._2.head))
        val headersToModifyMap = headers.groupBy(h => h.name()).map(entry => (entry._1, entry._2.head))
        val joinedMap = baseHeadersMap ++ headersToModifyMap
        joinedMap.values
      }
    }

    def overwrite(headers: izolotov.crawler.core.HttpHeader*): Iterable[izolotov.crawler.core.HttpHeader] => Iterable[izolotov.crawler.core.HttpHeader] = _ => headers

    def allow: URLAllowancePolicy.type = URLAllowancePolicy

    def disallow: URLDisallowancePolicy.type = URLDisallowancePolicy

    def follow: RedirectTrackingPolicy.type = RedirectTrackingPolicy

    def prevent: RedirectPreventingPolicy.type = RedirectPreventingPolicy

    // TODO good to make it private
    protected object URLAllowancePolicy {
      def all(): URL => Boolean = _ => true

      def host(host: String): URL => Boolean = url => url.getHost.toLowerCase == host.toLowerCase

      def url(url: String): URL => Boolean = _url => _url.toString.toLowerCase == url.toLowerCase
    }

    protected object URLDisallowancePolicy {
      def all(): URL => Boolean = _ => false

      def host(host: String): URL => Boolean = url => url.getHost.toLowerCase != host.toLowerCase

      def url(url: String): URL => Boolean = _url => _url.toString.toLowerCase != url.toLowerCase
    }

    protected object RedirectTrackingPolicy {

      def all(depth: Int = DefaultDepth): URL => Int = {
        require(depth >= 0)
        _ => depth
      }

      def host(host: String, depth: Int = DefaultDepth): URL => Int = {
        require(depth >= 0)
        _url => if (_url.getHost.toLowerCase == host.toLowerCase) depth else 0
      }

      def url(url: String, depth: Int = DefaultDepth): URL => Int = {
        require(depth >= 0)
        _url => if (_url.toString.toLowerCase == url.toLowerCase) depth else 0
      }
    }

    protected object RedirectPreventingPolicy {
      def all(): URL => Int = _ => 0

      def host(host: String, depth: Int = DefaultDepth): URL => Int = {
        require(depth >= 0)
        _url => if (_url.getHost.toLowerCase != host.toLowerCase) depth else 0
      }

      def url(url: String, depth: Int = DefaultDepth): URL => Int = {
        require(depth >= 0)
        _url => if (_url.toString.toLowerCase != url.toLowerCase) depth else 0
      }
    }
  }

  private val DaemonThreadFactory = new ThreadFactoryBuilder().setDaemon(true).build

  val DefaultDelay = 0L
  val DefaultTimeout = 10000L
  val PreventRedirectPolicy: URL => Int = _ => 0
  val RespectRobotsTxtPolicy: Boolean = true
  val AllowAllPolicy: URL => Boolean = _ => true

  private[crawler] case class Configuration[Raw, Doc] (
                                       parallelism: Int,
                                       redirect: Raw => Option[String],
                                       robotsHandler: Raw => RobotRules,
                                       queueLength: Int,
                                       allowancePolicy: URL => Boolean,
//                                       fetcher: (URL, Seq[HttpHeader]) => Raw,
                                       fetcher: URL => (URL, Iterable[HttpHeader]) => Raw,
                                       parser: URL => Raw => Doc,
                                       delay: URL => Long,
                                       timeout: URL => Long,
                                       redirectPolicy: URL => URL => Int,
                                       robotsTxtPolicy: URL => Boolean,
                                       httpHeaders: URL => Iterable[HttpHeader]
                                     )

  private[crawler] object ConfigurationBuilder {
    val PassHeaders: Iterable[HttpHeader] => Iterable[HttpHeader] = headers => headers
  }

  private[crawler] class ConfigurationBuilder[Raw, Doc](
                                                         val parallelism: Int,
                                                         // TODO rename to redirectExtractor or something like that
                                                         val redirectSpotter: Raw => Option[String],
                                                         val robotsHandler: Raw => RobotRules,
                                                         val fetcher: (URL, Iterable[HttpHeader]) => Raw,
                                                         val parser: Raw => Doc,
                                                         val allowancePolicy: URL => Boolean = AllowAllPolicy,
                                                         val queueLength: Int = Int.MaxValue,
                                                         val delay: Long = DefaultDelay,
                                                         val timeout: Long = DefaultTimeout,
                                                         val redirectPolicy: URL => Int = PreventRedirectPolicy,
                                                         val robotsTxtPolicy: Boolean = RespectRobotsTxtPolicy,
                                                         val headers: Iterable[HttpHeader] = Seq.empty
                                                       ) {
    private var getFetcher: PartialFunction[URL, (URL, Iterable[HttpHeader]) => Raw] = {case _ if true => this.fetcher}
    private var getParser: PartialFunction[URL, Raw => Doc] = {case _ if true => this.parser}
    private var getDelay: PartialFunction[URL, Long] = {case _ if true => this.delay}
    private var getTimeout: PartialFunction[URL, Long] = {case _ if true => this.timeout}
    private var getRedirectPolicy: PartialFunction[URL, URL => Int] = {case _ if true => this.redirectPolicy}
    private var getRespectRobotsTxt: PartialFunction[URL, Boolean] = {case _ if true => this.robotsTxtPolicy}
    private var getHeaders: PartialFunction[URL, Iterable[HttpHeader]] = {case _ if true => this.headers}

    def build(): Configuration[Raw, Doc] = {
      Configuration[Raw, Doc] (
        parallelism,
        redirectSpotter,
        robotsHandler,
        queueLength,
        allowancePolicy,
        getFetcher,
        getParser,
        getDelay,
        getTimeout,
        getRedirectPolicy,
        getRespectRobotsTxt,
        getHeaders
      )
    }

    def addConf (
                  predicate: URL => Boolean,
                  fetcher: (URL, Iterable[HttpHeader]) => Raw = this.fetcher,
                  parser: Raw => Doc = this.parser,
                  delay: Long = this.delay,
                  timeout: Long = this.timeout,
                  redirectPolicy: URL => Int = this.redirectPolicy,
                  robotsTxtPolicy: Boolean = this.robotsTxtPolicy,
                  headers: Iterable[HttpHeader] => Iterable[HttpHeader] = ConfigurationBuilder.PassHeaders,
                ): ConfigurationBuilder[Raw, Doc] = {
      // FIXME
      val fnRedirectPolicy: PartialFunction[URL, URL => Int] = {case url if predicate(url) => redirectPolicy}
      val fnFetcher: PartialFunction[URL, (URL, Iterable[HttpHeader]) => Raw] = {case url if predicate(url) => fetcher}
      if (fetcher != this.fetcher) getFetcher = fnFetcher.orElse(getFetcher)
      if (parser != this.parser) getParser = toPF(predicate, parser).orElse(getParser)
      if (delay != this.delay) getDelay = toPF(predicate, delay).orElse(getDelay)
      if (robotsTxtPolicy != this.robotsTxtPolicy) getRespectRobotsTxt = toPF(predicate, robotsTxtPolicy).orElse(getRespectRobotsTxt)
      if (timeout != this.timeout) getTimeout = toPF(predicate, timeout).orElse(getTimeout)
      if (redirectPolicy != this.redirectPolicy) getRedirectPolicy =
        fnRedirectPolicy.orElse(getRedirectPolicy)
      if (headers != ConfigurationBuilder.PassHeaders) getHeaders = toPF(predicate, headers(this.headers)).orElse(getHeaders)
      this
    }

    private def toPF[A](predicate: URL => Boolean, fn: A): PartialFunction[URL, A] = {
      case url if predicate(url) => fn
    }
  }

  object Crawler {
//    def read(urls: CrawlingQueueIterator): ExtractionBuilder = {
//      new ExtractionBuilder(urls)
//    }
    def read[A](urls: A)(implicit iteratorHelper: CrawlQueueHelper[A]): ExtractionBuilder = {
      new ExtractionBuilder(iteratorHelper.queue(urls))
    }
  }

  private[crawler] class ExtractionBuilder(urls: CrawlQueue) {
    def extract[Raw, Doc](
                           fetcher: (URL, Iterable[HttpHeader]) => Raw,
                           parser: Raw => Doc,
                           parallelism: Int = 10,
                           queueLength: Int = Int.MaxValue,
                           allowancePolicy: URL => Boolean = _ => true,
                           delay: Long = 0L,
                           timeout: Long = 30000L,
                           redirectPolicy: URL => Int = PreventRedirectPolicy,
                           respectRobotsTxt: Boolean = true,
                           headers: Iterable[HttpHeader] = Seq.empty
                         )(implicit redirect: Raw => Option[String], robots: Raw => RobotRules): BranchPredicateBuilder[Raw, Doc] = {
      val confBuilder = new ConfigurationBuilder(
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
        respectRobotsTxt,
        headers
      )
      new BranchPredicateBuilder[Raw, Doc](
        urls, confBuilder
      )
    }
  }

  private[crawler] class BranchPredicateBuilder[Raw, Doc](
                                                           urls: CrawlQueue,
                                                           builder: ConfigurationBuilder[Raw, Doc]
                                                         ) {
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
      Executors.newSingleThreadExecutor(DaemonThreadFactory)
    )

    def foreach[Err](onSuccess: Doc => Unit, onErr: Throwable => Err = (exc: Throwable) => throw exc): Unit = {
      foreachRaw(res => res.doc)(doc => doc.map(onSuccess).recover { case e if true => onErr(e) })
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

  private[crawler] class BranchConfigurationBuilder[Raw, Doc](urls: CrawlQueue, conf: ConfigurationBuilder[Raw, Doc], predicate: URL => Boolean) {
    def set(
             fetcher: (URL, Iterable[HttpHeader]) => Raw = conf.fetcher,
             parser: Raw => Doc = conf.parser,
             delay: Long = conf.delay,
             timeout: Long = conf.timeout,
             redirectPolicy: URL => Int = conf.redirectPolicy,
             respectRobotsTxt: Boolean = conf.robotsTxtPolicy,
             headers: Iterable[HttpHeader] => Iterable[HttpHeader] = ConfigurationBuilder.PassHeaders
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, fetcher, parser, delay, timeout, redirectPolicy, respectRobotsTxt, headers)
      new BranchPredicateBuilder[Raw, Doc](urls, conf)
    }
  }

}
