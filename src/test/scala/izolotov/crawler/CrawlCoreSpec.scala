package izolotov.crawler

import com.google.common.util.concurrent.ThreadFactoryBuilder
import izolotov.BoundedCrawlQueue
import izolotov.crawler.CrawlCoreSpec._
import izolotov.crawler.core.Api.{Configuration, ConfigurationBuilder}
import izolotov.crawler.core.{Attempt, HttpHeader}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.net.{MalformedURLException, URL}
import java.util.concurrent.{CopyOnWriteArrayList, Executors}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.jdk.CollectionConverters._
import scala.util.Success

object CrawlCoreSpec {

  case class Raw(url: String, redirect: Option[String] = None)

  object DummyRobotRules extends RobotRules {
    override def delay(): Option[Long] = Some(1000L)

    override def allowance(url: URL): Boolean = url.toString != YoutubeDisallowByRobots
  }

  val Google = "http://google.com"
  val Yahoo = "http://yahoo.com"
  val Openai = "http://openai.com"

  val Youtube = "http://youtube.com"
  val YoutubeRobots = "http://youtube.com/robots.txt"
  val DisallowByRobots = "disallowed-by-robots.txt"
  val YoutubeDisallowByRobots = s"http://youtube.com/$DisallowByRobots"

  val RedirectBase = "http://redirect.com"
  val RedirectDepth1 = s"$RedirectBase/1"
  val RedirectDepth2 = s"$RedirectBase/2"
  val RedirectDepth3 = s"$RedirectBase/3"

  val MalformedRedirectBase = "http://malformedredirect.com"
  val MalformedRedirectDepth1 = "http://malformed:redirect.com/1"


  def success(url: String): Attempt[Raw] = Attempt(url, Success(Raw(url, None)))
  def redirect(url: String, target: String): Attempt[Raw] = Attempt(url, Success(Raw(url, Some(target))), Some(target))

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

    def call(url: URL, headers: Iterable[HttpHeader]): Raw = {
      val startTs = System.currentTimeMillis()
      Thread.sleep(break)
      val request = Request(url, startTs, System.currentTimeMillis())
      _requests.add(request)
      ServerMock.Target(url.toString)
    }

    def requests: Iterable[Request] = _requests.asScala
  }
}

class CrawlCoreSpec extends AnyFlatSpec with BeforeAndAfterEach {

  implicit val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(
    Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setDaemon(true).build)
  )

  var server: ServerMock = _
  var conf: Configuration[Raw, Raw] = _

  override def beforeEach() {
    server = new ServerMock()
    val builder = new ConfigurationBuilder[Raw, Raw](
      parallelism = 2,
      redirect = raw => raw.redirect,
      robotsHandler = _ => DummyRobotRules,
      queueLength = 3,
      allowancePolicy = url => !url.toString.contains("disallowed"),
      fetcher = server.call,
      parser = req => req,
      delay = 20L,
      timeout = 100L,
      redirectPolicy = _ => 0,
      robotsTxtPolicy = false
    ).addConf(
      predicate = url => url.toString == RedirectBase,
      redirectPolicy = _ => 2
    ).addConf(
      predicate = url => url.toString == Youtube,
      robotsTxtPolicy = true
    ).addConf(
      predicate = url => url.toString == Google,
      delay = 1000L
    )
    conf = builder.build()
  }

  it should "create attempt for a invalid URL" in {
    val queue = BoundedCrawlQueue(Seq("no-protocol-url", "http://malformed:url").iterator)
    val core = CrawlCore(queue, conf)
    val expected = Seq(classOf[IllegalArgumentException], classOf[MalformedURLException])
    val actual = new CopyOnWriteArrayList[Class[_ <: Throwable]]()
    core.foreach(att => att.doc.recover { case exc if true => actual.add(exc.getClass) } )
    assert(expected == actual.asScala)
  }

  it should "try to extract all URLs from the queue" in {
    val queue = BoundedCrawlQueue(Seq(Google, Yahoo, Openai).iterator)
    val core = CrawlCore(queue, conf)
    val expected = Seq(success(Google), success(Yahoo), success(Openai))
    val actual = new CopyOnWriteArrayList[Attempt[Raw]]()
    core.foreach(att => actual.add(att))
    assert(expected.toSet == actual.asScala.toSet)
  }

  it should "try to extract redirect targets up to specific depth" in {
    val queue = BoundedCrawlQueue(Seq(RedirectBase).iterator)
    val core = CrawlCore(queue, conf)
    val expected = Seq((RedirectBase, Some(RedirectDepth1)), (RedirectDepth1, Some(RedirectDepth2)), (RedirectDepth2, Some(RedirectDepth3)))
    val actual = new CopyOnWriteArrayList[(String, Option[String])]()
    core.foreach(att => actual.add((att.url, att.redirectTarget)))
    assert(actual.asScala.toSet == expected.toSet)
  }

  it should "not try to extract redirect targets if the target is invalid" in {
    val queue = BoundedCrawlQueue(Seq(MalformedRedirectBase).iterator)
    val core = CrawlCore(queue, conf)
    val expected = Seq((MalformedRedirectBase, Some(MalformedRedirectDepth1)))
    val actual = new CopyOnWriteArrayList[(String, Option[String])]()
    core.foreach(att => actual.add((att.url, att.redirectTarget)))
    assert(actual.asScala.toSet == expected.toSet)
  }

}
