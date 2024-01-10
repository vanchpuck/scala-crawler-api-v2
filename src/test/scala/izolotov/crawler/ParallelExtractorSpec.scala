package izolotov.crawler

import izolotov.crawler.CrawlCoreSpec.{DummyRobotRules, Google, Raw, ServerMock, Youtube, YoutubeDisallowByRobots}
import izolotov.crawler.ParallelExtractor.NotAllowedByRobotsTxtException
import izolotov.crawler.ParallelExtractorSpec._
import izolotov.crawler.SuperNewCrawlerApi.{Configuration, ConfigurationBuilder}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URL
import java.util.concurrent.{CopyOnWriteArrayList, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

object ParallelExtractorSpec {

  case class Raw(url: String, redirect: Option[String])

  object DummyRobotRules extends RobotRules {
    override def delay(): Option[Long] = Some(1000L)

    override def allowance(url: URL): Boolean = url.toString != YoutubeDisallowByRobots
  }

  val TimeoutSeconds = 10L

  val Google = "http://google.com"
  val Yahoo = "http://yahoo.com"
  val Openai = "http://openai.com"

  val YoutubeHost = "youtube.com"
  val Youtube = s"http://$YoutubeHost.com"
  val YoutubeRobots = s"http://$YoutubeHost/robots.txt"
  val YoutubeDisallowByRobots = s"http://$YoutubeHost/disallowed-by-robots.txt"

  val RedirectBase = "http://redirect.com"
  val RedirectDepth1 = s"$RedirectBase/1"
  val RedirectDepth2 = s"$RedirectBase/2"
  val RedirectDepth3 = s"$RedirectBase/3"

  val MalformedRedirectBase = "http://malformedredirect.com"
  val MalformedRedirectDepth1 = "http://malformed:redirect.com/1"


  def success(url: String): Attempt[Raw] = Attempt(url, Success(Raw(url, None)))
  def redirect(url: String, target: String): Attempt[Raw] = Attempt(url, Success(Raw(url, Some(target))), Some(target))
  def failure(url: String, exception: Exception): Attempt[Raw] = Attempt(url, Failure(exception), None)

  case class Request(url: URL, startTs: Long, endTs: Long)

  object ServerMock {
    val Target: Map[String, Raw] = Map(
      Google -> Raw(Google, None),
      Yahoo -> Raw(Yahoo, None),
      Openai -> Raw(Openai, None),
      Youtube -> Raw(Youtube, None),
      YoutubeRobots -> Raw(YoutubeRobots, None),
      RedirectBase -> Raw(RedirectBase, Some(RedirectDepth1)),
      RedirectDepth1 -> Raw(RedirectDepth1, Some(RedirectDepth2)),
      RedirectDepth2 -> Raw(RedirectDepth2, Some(RedirectDepth3)),
      RedirectDepth3 -> Raw(RedirectDepth3, None),
      MalformedRedirectBase -> Raw(MalformedRedirectBase, Some(MalformedRedirectDepth1))
    )
  }

  class ServerMock(break: Long = 0L) {
    private val _requests = new CopyOnWriteArrayList[Request]()

    //    private def requests = _requests

    def call(url: URL): Raw = {
      val startTs = System.currentTimeMillis()
      Thread.sleep(break)
      val request = Request(url, startTs, System.currentTimeMillis())
      _requests.add(request)
      ServerMock.Target(url.toString)
    }

    def requests: Iterable[Request] = _requests.asScala
  }
}

class ParallelExtractorSpec extends AnyFlatSpec with BeforeAndAfterEach {


  var server: ServerMock = _
  var conf: Configuration[Raw, Raw] = _

  override def beforeEach() {
    server = new ServerMock()
    //    queue = new CrawlingQueue()
    var builder = new ConfigurationBuilder[Raw, Raw](
      parallelism = 2,
      redirect = raw => raw.redirect,
      robotsHandler = _ => DummyRobotRules,
      10,
      allowancePolicy = _ => true,
      fetcher = server.call,
      parser = req => req,
      delay = 20L,
      timeout = 1000000L,
      redirectPolicy = _ => 0,
      robotsTxtPolicy = false
    ).addConf(
      predicate = url => url.toString == RedirectBase,
      redirectPolicy = _ => 2
    ).addConf(
      predicate = url => url.getHost == YoutubeHost,
      robotsTxtPolicy = true,
    )
    conf = builder.build()
  }

  implicit val ec = ExecutionContext.global

  behavior of "ParallelExtractor"

  it should "keep the delay between first call to robots.txt and the subsequent call" in {

  }

  it should "not try to extract robots.txt if it's disabled" in {
    val extractor = new ParallelExtractor(conf)
    extractor.extract(Google)
    assert(!server.requests.exists(req => req.url.toString == YoutubeRobots))
  }

  it should "try to extract robots.txt if it's enabled" in {
    val extractor = new ParallelExtractor(conf)
    extractor.extract(Youtube)
    assert(server.requests.exists(req => req.url.toString == YoutubeRobots))
  }

  it should "follow the robots.txt rules if it's enabled" in {
    val extractor = new ParallelExtractor(conf)
    assert(Raw(Youtube) == Await.result(extractor.extract(Youtube), Duration.apply(TimeoutSeconds, TimeUnit.SECONDS)).doc.get)
    assertThrows[NotAllowedByRobotsTxtException](
      Await.result(extractor.extract(YoutubeDisallowByRobots), Duration.apply(TimeoutSeconds, TimeUnit.SECONDS)).doc.get
    )
  }

  it should "not follow the robots.txt rules if it's disabled" in {

  }

}
