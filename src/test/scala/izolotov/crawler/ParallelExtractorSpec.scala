package izolotov.crawler

import izolotov.crawler.ParallelExtractorSpec._
import izolotov.crawler.core.Api.{Configuration, ConfigurationBuilder}
import izolotov.crawler.core.ParallelExtractor.{NotAllowedByRobotsTxtException, NotAllowedByUserException, ParsingException, RedirectException}
import izolotov.crawler.core.{Attempt, HttpHeader, ParallelExtractor, RobotRules}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.net.{MalformedURLException, URL}
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Awaitable, ExecutionContext, ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object ParallelExtractorSpec {

  val DefaultProcessingQueueLength = 10

  val DefaultProcessingTime = 3L

  val DefaultUserDefinedDelay = 30L
  val YoutubeDelay = 20L

  val DefaultUserDefinedTimeout = 10L
  val GoogleTimeout = 1L

  case class Raw(url: String, redirect: Option[String] = None)

  object DummyRobotRules extends RobotRules {
    val DefaultDelay = 50L
    override def delay(): Option[Long] = Some(DefaultDelay)

    override def allowance(url: URL): Boolean = url.getPath.replaceFirst("^/", "") != DisallowByRobots
  }

  val TimeoutSeconds = 10L

  val DisallowByRobots = "disallowed-by-robots.txt"
  val DisallowByUser = "disallowed-by-user.txt"
  val FetchingException = "fetching-exception"

  val Google = "http://google.com"
  val GoogleFailing = s"$Google/$FetchingException"
  val Yahoo = "http://yahoo.com"

  val OpenaiHost = "openai.com"
  val Openai = s"http://$OpenaiHost"
  val OpenaiDisallowByRobots = s"$Openai/$DisallowByRobots"
  val OpenaiRobots = s"$Openai/robots.txt"
  val OpenaiTimeout = s"$Openai/timeout"

  val YoutubeHost = "youtube.com"
  val Youtube = s"http://$YoutubeHost"
  val YoutubeRobots = s"$Youtube/robots.txt"
  val YoutubeDisallowByRobots = s"$Youtube/$DisallowByRobots"
  val YoutubeDisallowByUser = s"$Youtube/$DisallowByUser"

  val Microsoft = "http://microsoft.com"

  val RedirectBase = "http://redirect.com"
  val RedirectDepth1 = s"$RedirectBase/1"
  val RedirectDepth2 = s"$RedirectBase/2"
  val RedirectDepth3 = s"$RedirectBase/3"

  val MalformedRedirectBase = "http://malformedredirect.com"
  val MalformedRedirectDepth1 = "http://malformed:redirect.com/1"

  val Apache = "http://apache.org"
  val Github = "http://github.com"
  val Gmail = "http://gmail.com"


  def success(url: String): Attempt[Raw] = Attempt(url, Success(Raw(url, None)))
  def redirect(url: String, target: String): Attempt[Raw] = Attempt(url, Success(Raw(url, Some(target))), Some(target))
  def failure(url: String, exception: Exception): Attempt[Raw] = Attempt(url, Failure(exception), None)

  case class Request(url: URL, startTs: Long, endTs: Long)

  object ServerMock {
    val Target: Map[String, Raw] = Map(
      Microsoft -> Raw(Microsoft),
      Google -> Raw(Google),
      Yahoo -> Raw(Yahoo),
      Openai -> Raw(Openai),
      OpenaiRobots -> Raw(OpenaiRobots),
      OpenaiDisallowByRobots -> Raw(OpenaiDisallowByRobots),
      Youtube -> Raw(Youtube),
      YoutubeRobots -> Raw(YoutubeRobots),
      YoutubeDisallowByRobots -> Raw(YoutubeDisallowByRobots),
      RedirectBase -> Raw(RedirectBase, Some(RedirectDepth1)),
      RedirectDepth1 -> Raw(RedirectDepth1, Some(RedirectDepth2)),
      RedirectDepth2 -> Raw(RedirectDepth2, Some(RedirectDepth3)),
      RedirectDepth3 -> Raw(RedirectDepth3),
      MalformedRedirectBase -> Raw(MalformedRedirectBase, Some(MalformedRedirectDepth1)),
      Apache -> Raw(Apache),
      Github -> Raw(Github),
      Gmail -> Raw(Gmail)
    )
  }

  class ServerMock(processingTime: Long = 0L) {
    private val _requests = Collections.synchronizedList(new util.ArrayList[Request]())

    def call(url: URL, headers: Iterable[HttpHeader]): Raw = {
      println("Call: " + url.toString)
      val startTs = System.currentTimeMillis()
      try {
        Thread.sleep(processingTime)
        if (url.getFile.endsWith(FetchingException)) throw new Exception()
        val raw = ServerMock.Target(url.toString)
        raw
      } finally
        _requests.add(Request(url, startTs, System.currentTimeMillis()))
    }

    def requests: Iterable[Request] = _requests.asScala
  }

  def await[A](fn: Awaitable[A], timeoutSec: Long = TimeoutSeconds): A = {
    Await.result(fn, Duration.apply(timeoutSec, TimeUnit.SECONDS))
  }

  def getMinDelay(requests: Seq[Request]): Long = {
    requests.sliding(2, 1).map(slide => slide(1).startTs - slide.head.endTs).min
  }
//  def await[A, B](awaitable: Awaitable[A], timeoutSec: Long): B => Await.result(awaitable, Duration.apply(TimeoutSeconds, TimeUnit.SECONDS))
}

class ParallelExtractorSpec extends AnyFlatSpec with BeforeAndAfterEach {


  var server: ServerMock = _
  var conf: Configuration[Raw, Raw] = _

  override def beforeEach() {
    server = new ServerMock(DefaultProcessingTime)
    val builder = new ConfigurationBuilder[Raw, Raw](
      parallelism = 1,
      redirectSpotter = raw => raw.redirect,
      robotsHandler = _ => DummyRobotRules,
      queueLength = DefaultProcessingQueueLength,
      allowancePolicy = url => !url.getFile.endsWith(DisallowByUser),
      fetcher = server.call,
      parser = req => req,
      delay = DefaultUserDefinedDelay,
      timeout = DefaultUserDefinedTimeout,
      redirectPolicy = _ => 0,
      robotsTxtPolicy = false
    ).addConf(
      predicate = url => url.toString == RedirectBase,
      redirectPolicy = _ => 2
    ).addConf(
      predicate = url => url.getHost == YoutubeHost,
      delay = YoutubeDelay,
      robotsTxtPolicy = true,
    ).addConf(
      predicate = url => url.getHost == OpenaiHost,
      robotsTxtPolicy = false,
    ).addConf(
      predicate = url => url.toString == Microsoft,
      parser = _ => throw new Exception("Dummy exception"),
    ).addConf(
      predicate = url => url.toString == Google,
      timeout = GoogleTimeout,
    ).addConf(
      predicate = url => url.toString == Apache,
      delay = 0L,
    ).addConf(
      predicate = url => url.toString == Github,
      delay = 0L,
    )
    conf = builder.build()
  }

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  behavior of "ParallelExtractor"

  it should "not try to extract robots.txt if it's disabled" in {
    val extractor = new ParallelExtractor(conf)
    await(extractor.extract(Openai))
    assert(!server.requests.exists(req => req.url.toString == OpenaiRobots))
  }

  it should "try to extract robots.txt if it's enabled" in {
    await(new ParallelExtractor(conf).extract(Youtube))
    assert(server.requests.exists(req => req.url.toString == YoutubeRobots))
  }

  it should "try to extract robots.txt only once per host if it's enabled" in {
    val extractor = new ParallelExtractor(conf)
    await(extractor.extract(Youtube))
    await(extractor.extract(Youtube))
    assert(server.requests.count(req => req.url.toString == YoutubeRobots) == 1)
  }

  it should "follow the robots.txt rules if it's enabled" in {
    val extractor = new ParallelExtractor(conf)
    assert(Raw(Youtube) == await(extractor.extract(Youtube)).doc.get)
    assertThrows[NotAllowedByRobotsTxtException](
      await(extractor.extract(YoutubeDisallowByRobots)).doc.get
    )
  }

  it should "not follow the robots.txt rules if it's disabled" in {
    assert(Raw(OpenaiDisallowByRobots) == await(new ParallelExtractor(conf).extract(OpenaiDisallowByRobots)).doc.get)
  }

  it should "keep the delay between first call to robots.txt and the subsequent call" in {
    await(new ParallelExtractor(conf).extract(Youtube))
    assert(getMinDelay(server.requests.toSeq) >= DummyRobotRules.DefaultDelay)
  }

  it should "return an attempt with a failure for a URL that can't be extracted" in {
    val extractor = new ParallelExtractor(conf)
    assertThrows[IllegalArgumentException](await(extractor.extract("no-protocol-url")).doc.get)
    assertThrows[MalformedURLException](await(extractor.extract("http://malformed:url")).doc.get)
    assertThrows[ParallelExtractor.FetchingException](await(extractor.extract(GoogleFailing)).doc.get)
  }

  it should "return an attempt with a raw response for a URL that can't be parsed" in {
    val extractor = new ParallelExtractor(conf)
    await(extractor.extract(Microsoft)).doc match {
      case Failure(exc) => assert(exc.asInstanceOf[ParsingException[Raw]].raw == Raw(Microsoft))
    }
  }

  it should "return an attempt with a raw response for a URL with redirect" in {
    val extractor = new ParallelExtractor(conf)
    await(extractor.extract(RedirectBase)).doc match {
      case Failure(exc) => assert(exc.asInstanceOf[RedirectException[Raw]].raw == Raw(RedirectBase, Some(RedirectDepth1)))
    }
  }

  it should "keep the delay between calls to the same hosts URLs" in {
    val extractor = new ParallelExtractor(conf)
    await(Future.sequence(Seq(
      extractor.extract(Openai),
      extractor.extract(Openai),
      extractor.extract(Openai),
      extractor.extract(Youtube),
      extractor.extract(Youtube),
      extractor.extract(Youtube)
    )))
    val grouped = server.requests.toSeq.groupBy(req => req.url.getHost)
    getMinDelay(grouped(YoutubeHost)) >= YoutubeDelay
    getMinDelay(grouped(OpenaiHost)) >= DefaultUserDefinedDelay
  }

  it should "fail with timeout if extraction takes too long" in {
    val extractor = new ParallelExtractor(conf)
    val actual = await(extractor.extract(Google)).doc.failed.get.getClass
    val expected = classOf[ParallelExtractor.TimeoutException]
    assert(expected == actual)
  }

  it should "respect a user-defined allowance policy" in {
    await(new ParallelExtractor(conf).extract(YoutubeDisallowByUser))
    assertThrows[NotAllowedByUserException](
      await(new ParallelExtractor(conf).extract(YoutubeDisallowByUser)).doc.get
    )
    assert(server.requests.isEmpty)
  }

  it should "Extract a specified max number of URLs in parallel" in {
    val extractor = new ParallelExtractor(conf)
    await(Future.sequence(Seq(
      extractor.extract(Apache),
      extractor.extract(Github),
      extractor.extract(Gmail)
    )))
    val requests = server.requests.toSeq.sortBy(req => req.startTs)
    println(requests)
    assert(requests(2).startTs >= Math.min(requests.head.endTs, requests(1).endTs))
  }

  it should "not support queueLength value that is less than 1" in {
    val conf = new ConfigurationBuilder[Raw, Raw](
      parallelism = 1,
      redirectSpotter = raw => raw.redirect,
      robotsHandler = _ => DummyRobotRules,
      queueLength = 0,
      fetcher = server.call,
      parser = req => req,
      robotsTxtPolicy = false,
      delay = 5L
    ).build()
    assertThrows[IllegalArgumentException](
      new ParallelExtractor(conf)
    )
  }

}
