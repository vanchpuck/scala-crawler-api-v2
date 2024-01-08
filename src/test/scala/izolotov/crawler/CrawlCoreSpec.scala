package izolotov.crawler

import izolotov.CrawlingQueue
import izolotov.crawler.CrawlCoreSpec._
import izolotov.crawler.SuperNewCrawlerApi.{Configuration, ConfigurationBuilder}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.net.{MalformedURLException, URL}
import java.util.concurrent.CopyOnWriteArrayList
import scala.jdk.CollectionConverters._
import scala.util.Success

object CrawlCoreSpec {

  val Google = "http://google.com"
  val Yahoo = "http://yahoo.com"
  val Openai = "http://openai.com"
  val Redirect = "http://redirect.com"
  val Target = "http://target.com"
  val TargetDepth1 = "http://target.com/2"
  val TargetDepth2 = "http://target.com/2"

  def success(url: String): Attempt[Unit] = Attempt(url, Success(()))
  def redirect(url: String, target: String): Attempt[Unit] = Attempt(url, Success(()), Some(target))

  case class Request(url: URL, startTs: Long, endTs: Long)

  class ServerMock(break: Long = 0L) {
    private var _requests = new CopyOnWriteArrayList[Request]()

//    private def requests = _requests

    def call(url: URL): Request = {
      //      println("Call")
      val startTs = System.currentTimeMillis()
      Thread.sleep(break)
      val request = Request(url, startTs, System.currentTimeMillis())
      //      Request(url, startTs, System.currentTimeMillis())
      _requests.add(Request(url, startTs, System.currentTimeMillis()))
      request
    }

    def requests: Iterable[Request] = _requests.asScala
  }
}

class CrawlCoreSpec extends AnyFlatSpec with BeforeAndAfterEach {



  var server: ServerMock = _
//  var queue: ParallelExtractor.Queue = _
  var conf: Configuration[Request, Request] = _
//  var builder = new ConfigurationBuilder[Request, Request](
//    parallelism = 2,
//    redirect = req => if (req.url.toString == "http://redirect.com") Some("http://target.com") else None,
//    robotsHandler = req => RobotRules.empty(),
//    10,
//    allowancePolicy = url => !url.toString.contains("disallowed"),
//    fetcher = server.call,
//    parser = req => req,
//    delay = 20L,
//    timeout = 100L,
//    redirectPolicy = _ => 1,
//    robotsTxtPolicy = false
//  )



  override def beforeEach() {
    server = new ServerMock()
//    queue = new CrawlingQueue()
    var builder = new ConfigurationBuilder[Request, Request](
      parallelism = 2,
      redirect = req => Some(s"${Target}/${Option(req.url.getFile).filter(_.trim.nonEmpty).map(f => f.substring(1).toInt + 1).getOrElse(1)}"),
      robotsHandler = _ => RobotRules.empty(),
      10,
      allowancePolicy = url => !url.toString.contains("disallowed"),
      fetcher = server.call,
      parser = req => req,
      delay = 20L,
      timeout = 100L,
      redirectPolicy = _ => 0,
      robotsTxtPolicy = false
    ).addConf(
      predicate = url => url.toString == Redirect,
      redirectPolicy = _ => 3
    )
    conf = builder.build()
  }

//  override def afterEach() {
//    queue.close()
//  }

  it should "create attempt for a invalid URL" in {
    val queue = new CrawlingQueue(Seq("no-protocol-url", "http://malformed:url"))
    val core = CrawlCore(queue, conf)
    val expected = Seq(classOf[IllegalArgumentException], classOf[MalformedURLException])
    val actual = new CopyOnWriteArrayList[Class[_ <: Throwable]]()
    core.foreach(att => att.doc.recover { case exc if true => actual.add(exc.getClass) } )
    assert(expected == actual.asScala)
  }

  it should "try to extract all URLs from the queue" in {
    val queue = new CrawlingQueue(Seq(Google, Yahoo, Openai))
    val core = CrawlCore(queue, conf)
    val expected = Seq(success(Google), success(Yahoo), success(Openai))
    val actual = new CopyOnWriteArrayList[Attempt[Request]]()
    core.foreach(att => actual.add(att))
    assert(expected.toSet == actual.asScala.toSet)
  }

  it should "try to extract redirect targets up to specific depth" in {
//    val queue = new CrawlingQueue(Seq(Redirect))
//    val core = CrawlCore(queue, conf)
//    val expected = Seq(redirect(Redirect, TargetDepth1), redirect(TargetDepth1, TargetDepth2), success(TargetDepth2))
//    val actual = new CopyOnWriteArrayList[Attempt[Request]]()
//    core.foreach(att => actual.add(att))
//    println(actual)
//    assert(expected.toSet == actual.asScala.toSet)
  }

  it should "not try to extract redirect targets if redirects are ignored" in {}

  it should "not try to extract redirect targets if the target is invalid" in {}

}
