package izolotov.crawler

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.DelayedApplier
import izolotov.crawler.ParallelExtractor._
//import izolotov.crawler.RobotRules.RobotRulesStub
import izolotov.crawler.SuperNewCrawlerApi.Configuration

import java.net.{URI, URL}
import java.util.concurrent._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}

object ParallelExtractor {

  class HostQueueIsFullException(cause: String) extends RejectedExecutionException

  class ProcessingQueueIsFullException(cause: String) extends RejectedExecutionException


  val DaemonThreadFactory = new ThreadFactoryBuilder().setDaemon(true).build

  // TODO add initial delay parameter
  class TimeoutException(message: String) extends Exception(message: String)

  class NotAllowedException(url: URL) extends Exception
  class NotAllowedByRobotsTxtException(url: URL) extends NotAllowedException(url)

  class NotAllowedByUserSettingsException(url: URL) extends NotAllowedException(url)

//  class Result[Doc](doc: Doc, redirect: Option[URL], outLinks: Iterable[URL])


//  case class Result[Doc](url: String, doc: Try[Doc], redirectTarget: Option[String], outLinks: Iterable[String])

  class ParsingException[Raw](raw: Raw) extends Exception

  class FetchingException(cause: Throwable) extends Exception

  object Queue {
    def apply(capacity: Int = Int.MaxValue): Queue = new Queue(capacity)
    def apply(): Queue = new Queue()
  }

  class Queue(capacity: Int = Int.MaxValue) {

    private val localEC = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1, DaemonThreadFactory))
    private val extractionEC = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1, DaemonThreadFactory))

//    private val extractionExecutor = Executors.newFixedThreadPool(1, DaemonThreadFactory)

    private val applier = new DelayedApplier

    private var isClosed = false

    def extract[Doc](
                      url: URL,
                      fnExtract: URL => Doc,
                      delay: Long = 0L,
                      timeout: Long = 10000L
                    )(implicit ec: ExecutionContext): Future[Doc] = {
      if (isClosed) throw new IllegalStateException("Queue is closed")
      Future {
        val f = Future {
          try
            Await.result(
              Future{applier.apply(url, fnExtract, delay)}(extractionEC),
              Duration.apply(timeout, TimeUnit.MILLISECONDS)
            )
          catch {
            case _: concurrent.TimeoutException =>
              throw new TimeoutException(s"URL ${url.toString} could not be extracted within ${timeout} milliseconds timeout")
          }

        }(ec)
        // TODO add timeout parameter along with delay
        Await.result(f, Duration.Inf)
      }(localEC)
    }

    def close(): Unit = {
      isClosed = true
      ParallelExtractor.shutdown(localEC)
      ParallelExtractor.shutdown(extractionEC)
    }
  }

  private def shutdown(executor: ExecutionContextExecutorService): Unit = {
    executor.shutdown
    try
      if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
        executor.shutdownNow
      }
    catch {
      case _: InterruptedException => {
        executor.shutdownNow
        Thread.currentThread.interrupt()
      }
    }
  }

  def getRobotsTxtURL: URL => URL = url => new URL(s"${url.getProtocol}://${url.getHost}/robots.txt")
}

class ParallelExtractor[Raw, Doc](conf: Configuration[Raw, Doc]) {

  object ExtractionTask {
    def start(url: URL): Attempt[Doc] = {
      val raw = Try(conf.fetcher(url)).recover{case e if true => throw new FetchingException(e)}
      val redirectTarget: Option[String] = raw.toOption.flatMap(conf.redirect)
      val doc = raw.map(r => conf.parser(url)(r))
      Attempt[Doc](url.toString, doc, redirectTarget, Seq.empty)
    }
  }

  implicit val ec = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
    conf.parallelism,
    conf.parallelism,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingDeque[Runnable](conf.queueLength),
    DaemonThreadFactory,
    (r: Runnable, e: ThreadPoolExecutor) => {
      // TODO block instead of throw an exception
      throw new ProcessingQueueIsFullException(s"Task ${r.toString} rejected from ${e.toString}")
    }
  ))

  private val hostMap = collection.mutable.Map[String, (Queue, _ <: RobotRules)]()

  private def extractRobotRules(url: URL, queue: Queue): RobotRules = {
    Try(Await.result(queue.extract(getRobotsTxtURL(url), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf))
      // TODO replace println with logging
      .recover { case exc if true => print(exc); RobotRules.empty() }
      .get
  }

  def extract(url: String): Future[Attempt[Doc]] = {
//    val a: String = hostMap("s") = null
    // TODO Improvement. We have to call robots.txt even if it's ignored in config.
    val spec = Try(URI.create(url).toURL)
      .map { _url => if (!conf.allowancePolicy(_url)) throw new Exception("Not allowed"); _url }
      .map { _url =>
        hostMap.get(_url.getHost).map { value =>
          val (queue, rules) = value
          rules match {
            case RobotRules.Stub =>
              if (conf.robotsTxtPolicy(_url)) {
                val actualRules = extractRobotRules(_url, queue)
                hostMap(url) = (queue, actualRules)
                (_url, queue, actualRules)
              } else
                (_url, queue, rules)
            case _ => (_url, queue, rules)
          }
        }.getOrElse {
          val queue = new Queue(conf.queueLength)
          val rules = if (conf.robotsTxtPolicy(_url)) extractRobotRules(_url, queue) else RobotRules.Stub
          (_url, queue, rules)
        }
      }
//      spec.
//        val (queue, rules) = hostMap.getOrElseUpdate(
//          _url.getHost,
//          {
//            val queue = new Queue()
//            val rules = if (conf.robotsTxtPolicy(_url))
//              Try(Await.result(new Queue().extract(getRobotsTxtURL(_url), conf.fetcher.andThen(conf.robotsHandler)), Duration.Inf))
//                // TODO replace println with logging
//                .recover { case exc if true => print(exc); RobotRules.empty() }
//                .get
//            else
//              RobotRules.stub()
//            (queue, rules)
//          }
//        )
//        (_url, queue, rules)
//      }
//      .map { _spec => val (_url, queue, _) = _spec; if (conf.robotsTxtPolicy(_url)) _spec else (_url, queue, RobotRules.empty()) }
      .map { _spec => val (_url, _, rules) = _spec; if (!rules.allowance(_url)) throw new NotAllowedByRobotsTxtException(_url); _spec }
    spec match {
      case Success((_url, queue, rules)) => queue.extract(_url, ExtractionTask.start, rules.delay().getOrElse(conf.delay(_url)))
      case Failure(exc) => Future.fromTry(Success(Attempt(url, Failure(exc), None, Seq.empty)))
    }
  }

  def shutdown(): Unit = {
    ParallelExtractor.shutdown(ec)
  }

}
