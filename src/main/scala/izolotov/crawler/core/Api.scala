package izolotov.crawler.core

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.crawler.{CrawlCore, RobotRules}
import izolotov.CrawlQueue

import java.net.URL
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}

object Api {

  private val DaemonThreadFactory = new ThreadFactoryBuilder().setDaemon(true).build

  val DefaultDelay = 0L
  val DefaultTimeout = 10000L
  val PreventRedirectPolicy: URL => Int = _ => 0
  val RespectRobotsTxtPolicy: Boolean = true
  val AllowAllPolicy: URL => Boolean = _ => true

  case class Configuration[Raw, Doc] (
                                       parallelism: Int,
                                       redirect: Raw => Option[String],
                                       robotsHandler: Raw => RobotRules,
                                       queueLength: Int,
                                       allowancePolicy: URL => Boolean,
//                                       fetcher: (URL, Seq[HttpHeader]) => Raw,
                                       fetcher: URL => (URL, Seq[HttpHeader]) => Raw,
                                       parser: URL => Raw => Doc,
                                       delay: URL => Long,
                                       timeout: URL => Long,
                                       redirectPolicy: URL => URL => Int,
                                       robotsTxtPolicy: URL => Boolean,
                                       httpHeaders: URL => Seq[HttpHeader]
                                     )

  // TODO refactor to class (not a case class)
  class ConfigurationBuilder[Raw, Doc](
                                        val parallelism: Int,
                                        // TODO rename to redirectExtractor or something like that
                                        val redirect: Raw => Option[String],
                                        val robotsHandler: Raw => RobotRules,
                                        val fetcher: (URL, Seq[HttpHeader]) => Raw,
                                        val parser: Raw => Doc,
                                        val allowancePolicy: URL => Boolean = AllowAllPolicy,
                                        val queueLength: Int = Int.MaxValue,
                                        val delay: Long = DefaultDelay,
                                        val timeout: Long = DefaultTimeout,
                                        val redirectPolicy: URL => Int = PreventRedirectPolicy,
                                        val robotsTxtPolicy: Boolean = RespectRobotsTxtPolicy,
                                        val httpHeaders: Seq[HttpHeader] = Seq.empty
                                      ) {
//    private var getFetcher: PartialFunction[(URL, Seq[HttpHeader]), Raw] = {case args if true => this.fetcher(args._1, httpHeaders)}
    private var getFetcher: PartialFunction[URL, (URL, Seq[HttpHeader]) => Raw] = {case _ if true => this.fetcher}
//    private var getFetcher: PartialFunction[URL, Raw] = {case url if true => this.fetcher(url, httpHeaders)}
    private var getParser: PartialFunction[URL, Raw => Doc] = {case _ if true => this.parser}
    private var getDelay: PartialFunction[URL, Long] = {case _ if true => this.delay}
    private var getTimeout: PartialFunction[URL, Long] = {case _ if true => this.timeout}
    private var getRedirectPolicy: PartialFunction[URL, URL => Int] = {case _ if true => this.redirectPolicy}
//    private var getRobotsTxtPolicy: PartialFunction[URL, URL => Int] = {case _ if true => this.robotsTxtPolicy}
    private var getRespectRobotsTxt: PartialFunction[URL, Boolean] = {case _ if true => this.robotsTxtPolicy}
    private var getHttpHeaders: PartialFunction[URL, Seq[HttpHeader]] = {case _ if true => this.httpHeaders}
    //    private var getAllowancePolicy: PartialFunction[URL, URL => Boolean] = {case _ if true => this.allowancePolicy}

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
        getRespectRobotsTxt,
        getHttpHeaders
      )
    }

    def addConf (
                  predicate: URL => Boolean,
                  fetcher: (URL, Seq[HttpHeader]) => Raw = this.fetcher,
                  parser: Raw => Doc = this.parser,
                  delay: Long = this.delay,
                  timeout: Long = this.timeout,
                  redirectPolicy: URL => Int = this.redirectPolicy,
                  robotsTxtPolicy: Boolean = this.robotsTxtPolicy,
                  httpHeaders: Seq[HttpHeader] = this.httpHeaders,
                ): ConfigurationBuilder[Raw, Doc] = {
      // FIXME
      val fnRedirectPolicy: PartialFunction[URL, URL => Int] = {case url if predicate(url) => redirectPolicy}
      //      val fnRobotsTxtPolicy: PartialFunction[URL, Boolean] = {case url if predicate(url) => robotsTxtPolicy(url)}
//      val fnFetcher: PartialFunction[URL, Raw] = {case url if predicate(url) => fetcher(url)}
      val fnFetcher: PartialFunction[URL, (URL, Seq[HttpHeader]) => Raw] = {case url if predicate(url) => fetcher}
      if (fetcher != this.fetcher) getFetcher = fnFetcher.orElse(getFetcher)
      if (parser != this.parser) getParser = toPF(predicate, parser).orElse(getParser)
      if (delay != this.delay) getDelay = toPF(predicate, delay).orElse(getDelay)
      if (robotsTxtPolicy != this.robotsTxtPolicy) getRespectRobotsTxt = toPF(predicate, robotsTxtPolicy).orElse(getRespectRobotsTxt)
      //      if (allowancePolicy != this.allowancePolicy) getAllowancePolicy = toPF(predicate, allowancePolicy).orElse(getAllowancePolicy)
      if (timeout != this.timeout) getTimeout = toPF(predicate, timeout).orElse(getTimeout)
      if (redirectPolicy != this.redirectPolicy) getRedirectPolicy =
        fnRedirectPolicy.orElse(getRedirectPolicy)
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

  class ExtractionBuilder(urls: CrawlQueue) {
    def extract[Raw, Doc](
                           fetcher: (URL, Seq[HttpHeader]) => Raw,
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
        respectRobotsTxt
      )
      new BranchPredicateBuilder[Raw, Doc](
        urls, confBuilder
      )
    }
  }

  class BranchPredicateBuilder[Raw, Doc](
                                          urls: CrawlQueue,
                                          builder: ConfigurationBuilder[Raw, Doc]
                                        ) {
    implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
      Executors.newSingleThreadExecutor(DaemonThreadFactory)
    )

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

  class BranchConfigurationBuilder[Raw, Doc](urls: CrawlQueue, conf: ConfigurationBuilder[Raw, Doc], predicate: URL => Boolean) {
    def set(
             fetcher: (URL, Seq[HttpHeader]) => Raw = conf.fetcher,
             parser: Raw => Doc = conf.parser,
             delay: Long = conf.delay,
             timeout: Long = conf.timeout,
             redirectPolicy: URL => Int = conf.redirectPolicy,
             respectRobotsTxt: Boolean = conf.robotsTxtPolicy,
             httpHeaders: Seq[HttpHeader] = conf.httpHeaders
           ): BranchPredicateBuilder[Raw, Doc] = {
      conf.addConf(predicate, fetcher, parser, delay, timeout, redirectPolicy, respectRobotsTxt)
      new BranchPredicateBuilder[Raw, Doc](urls, conf)
    }
  }

}
