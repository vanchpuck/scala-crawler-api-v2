package izolotov.crawler.core

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.crawler.core.ParallelExtractor._
import izolotov.crawler.core.Api.Configuration

import java.net.{URI, URL}
import java.util.concurrent._
import java.util.concurrent.locks.{Condition, ReentrantLock}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.{Failure, Success, Try}

object ParallelExtractor {

  private val DaemonThreadFactory = new ThreadFactoryBuilder().setDaemon(true).build

  // TODO add initial delay parameter
  class TimeoutException(message: String) extends Exception(message: String)

  class NotAllowedException(message: String) extends Exception(message)
  class NotAllowedByRobotsTxtException(url: URL) extends NotAllowedException(s"${url.toString} is not allowed by robots.txt policy")

  class NotAllowedByUserException(url: URL) extends NotAllowedException(s"${url.toString} is not allowed by user policy")

  class NotParsedException[Raw](val url: URL, val raw: Raw, val message: String) extends Exception(message)

  class RedirectException[Raw](url: URL, raw: Raw, val target: String) extends NotParsedException(url, raw, s"${url.toString} has been redirected to $target")

  class ParsingException[Raw](url: URL, raw: Raw, val cause: Throwable) extends NotParsedException[Raw](url, raw, s"${url.toString} parsing exception $cause")

  class FetchingException(cause: Throwable) extends Exception(cause)

  object Queue {
    def apply(): Queue = new Queue()
  }

  class Queue {

    private val localEC = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor(DaemonThreadFactory))

    private val applier = new DelayedApplier

    private var isClosed = false

    def extract[Doc](
                      url: URL,
                      fnExtract: URL => Doc,
                      delay: Long = 0L
                    )(implicit ec: ExecutionContext): Future[Doc] = {
      if (isClosed) throw new IllegalStateException("Queue is closed")
      val future = Future {
        val f = Future(applier.apply(url, fnExtract, delay))(ec)
        Await.result(f, Duration.Inf)
      }(localEC)
      future
    }

    def close(): Unit = {
      isClosed = true
      ParallelExtractor.shutdown(localEC)
    }
  }

  private def shutdown(executor: ExecutionContextExecutorService): Unit = {
    executor.shutdown()
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

  private def getRobotsTxtURL: URL => URL = url => URI.create(s"${url.getProtocol}://${url.getHost}${if (url.getPort != -1) s":${url.getPort}" else ""}/robots.txt").toURL
}

class ParallelExtractor[Raw, Doc](conf: Configuration[Raw, Doc]) {

  if (conf.queueLength < 1) throw new IllegalArgumentException("Queue length must be grater then 0")

  class AttemptStarter(fnGetTimeout: URL => Long) extends AutoCloseable {
    private implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(Executors.newThreadPerTaskExecutor(DaemonThreadFactory))
    def start(url: URL): Attempt[Doc] = {
      val timeout = fnGetTimeout(url)
      val future = Future {
        val raw = Try(conf.fetcher(url)(url, conf.httpHeaders(url))).recover { case e if true => throw new FetchingException(e) }
        val redirectTarget: Option[String] = raw.toOption.flatMap(conf.redirect)
        val doc = redirectTarget
          .map(target => Failure(new RedirectException(url, raw.get, target)))
          .getOrElse(Try(conf.parser(url)(raw.get)).recover{ case e if true => throw new ParsingException[Raw](url, raw.get, e) })
        Attempt[Doc](url.toString, doc, redirectTarget, Seq.empty)
      }
      Try(Await.result(future, Duration.apply(timeout, TimeUnit.MILLISECONDS)))
        .recover {
          case _ =>
            val timeoutExc = new ParallelExtractor.TimeoutException(s"URL ${url.toString} could not be extracted within $timeout milliseconds timeout")
            Attempt[Doc](url.toString, Failure(timeoutExc), None, Seq.empty)
        }
        .get
    }

    override def close(): Unit = ParallelExtractor.shutdown(ec)
  }

  // TODO rename!!!
  object St extends RobotRules {
    override def delay(): Option[Long] = RobotRules.Empty.delay()

    override def allowance(url: URL): Boolean = RobotRules.Empty.allowance(url)
  }

  private val ec = ExecutionContext.fromExecutorService(new ThreadPoolExecutor(
    conf.parallelism,
    conf.parallelism,
    0L,
    TimeUnit.MILLISECONDS,
    new LinkedBlockingDeque[Runnable](conf.queueLength),
    DaemonThreadFactory,
    (r: Runnable, e: ThreadPoolExecutor) => {
      lock.lock()
      Try(condition.await())
      lock.unlock()
      e.execute(r)
    }
  ))

  private val hostMap = collection.mutable.Map[String, (Queue, _ <: RobotRules)]()
  private val attemptStarter = new AttemptStarter(conf.timeout)

  private val lock: ReentrantLock = new ReentrantLock()
  private val condition: Condition = lock.newCondition()

  private def extractRobotRules(url: URL, queue: Queue): RobotRules = {
    val robotsTxtUrl = getRobotsTxtURL(url)
    Try(Await.result(
      queue.extract(
        robotsTxtUrl,
        conf.fetcher.andThen(a => a(robotsTxtUrl, conf.httpHeaders(robotsTxtUrl))).andThen(conf.robotsHandler)
      )(ec),
      Duration.Inf
    )).recover { case exc if true => print(exc); RobotRules.Empty }.get // TODO replace println with logging
  }

  def extract(url: String): Future[Attempt[Doc]] = {
    val spec = Try(URI.create(url).toURL)
      .map { _url => if (!conf.allowancePolicy(_url)) throw new NotAllowedByUserException(_url); _url }
      .map { _url =>
        hostMap.get(_url.getHost).map { value =>
          val (queue, rules) = value
          rules match {
            case St =>
              if (conf.robotsTxtPolicy(_url)) {
                val actualRules = extractRobotRules(_url, queue)
                hostMap(_url.getHost) = (queue, actualRules)
                (_url, queue, actualRules)
              } else
                (_url, queue, rules)
            case _ => (_url, queue, rules)
          }
        }.getOrElse {
          val queue = new Queue()
          val rules = if (conf.robotsTxtPolicy(_url)) {
            extractRobotRules(_url, queue)
          } else {
            St
          }
          hostMap(_url.getHost) = (queue, rules)
          (_url, queue, rules)
        }
      }
      .map { _spec => val (_url, _, rules) = _spec; if (!rules.allowance(_url)) throw new NotAllowedByRobotsTxtException(_url); _spec }
    val future = spec match {
      case Success((_url, queue, rules)) => queue.extract(_url, attemptStarter.start, rules.delay().getOrElse(conf.delay(_url)))(ec)
      case Failure(exc) =>
        val ftr: Future[Attempt[Doc]] = Future.fromTry(Success(Attempt(url, Failure(exc), None, Seq.empty)))
        ftr
    }
    future.onComplete { _ =>
      lock.lock()
      Try(condition.signal())
      lock.unlock()
    }(ec)
    future
  }

  def shutdown(): Unit = {
    ParallelExtractor.shutdown(ec)
  }

}
