package izolotov.crawler

import izolotov.crawler.core.Api.ConfigurationBuilder
import org.scalatest.flatspec.AnyFlatSpec
import ApiSpec._
import com.google.common.net.HttpHeaders
import izolotov.crawler.core.{Api, BasicHttpHeader, HttpHeader, RobotRules}
import izolotov.crawler.core.Api.Util._

import java.net.{URI, URL}

object ApiSpec {

  val Parallelism: Int = 1
  val Redirect: String => Option[String] = _ => None

  val DefaultFetcherOutput = "Default fetcher output"
  val DefaultParserOutput = "Default parser output"

  val FooNet = "Foo.net"
  val BarCom = "Bxar.com"
  val FooNetUrl: URL = URI.create(s"http://$FooNet").toURL
  val BarComUrl: URL = URI.create(s"http://$BarCom").toURL

  private val ExampleCom: URL = URI.create("http://example.com").toURL
  private val ExampleComStr = ExampleCom.toString
  private val ExampleComFetcher: (URL, Iterable[HttpHeader]) => String = (a, _) => a.toString
  private val ExampleComParser: String => String = raw => raw

  private val GoogleCom: URL = URI.create("http://google.com").toURL
  private val GoogleComStr = GoogleCom.toString
  private val GoogleComPredicate: URL => Boolean = url(GoogleCom.toString)
  private val GoogleComFetcher: (URL, Iterable[HttpHeader]) => String = (a, _) => a.toString
  private val GoogleComParser: String => String = raw => raw
  private val GoogleComDelay = 1L
  private val GoogleComTimeout = 1L
  private val GoogleComRedirectPolicy: URL => Int = _ => 1

  private val DoNotRespectRobotsTxtPolicy = false

  private val Conf = new ConfigurationBuilder[String ,String](
    parallelism = Parallelism,
    redirectSpotter = Redirect,
    robotsHandler = _ => RobotRules.Empty,
    fetcher = ExampleComFetcher,
    parser = raw => raw
  ).addConf(
    predicate = GoogleComPredicate,
    fetcher = GoogleComFetcher,
    parser = GoogleComParser,
    delay = GoogleComDelay,
    timeout = GoogleComTimeout,
    redirectPolicy = GoogleComRedirectPolicy,
    robotsTxtPolicy = DoNotRespectRobotsTxtPolicy,
  ).build()
}

class ApiSpec extends AnyFlatSpec {

  behavior of "ConfigurationBuilder"

  it should "build the configuration with the correct default parameters" in {
    assert(Conf.parallelism == Parallelism)
    assert(Conf.redirect == Redirect)
    assert(Conf.robotsHandler(ExampleComStr) == RobotRules.Empty)
    assert(Conf.fetcher(ExampleCom)(ExampleCom, Seq(Util.DummyHeader)) == ExampleComFetcher(ExampleCom, Seq(Util.DummyHeader)))
    assert(Conf.parser(ExampleCom)(ExampleComStr) == ExampleComParser(ExampleComStr))
    assert(Conf.allowancePolicy(ExampleCom))
    assert(Conf.queueLength == Int.MaxValue)
    assert(Conf.delay(ExampleCom) == Api.DefaultDelay)
    assert(Conf.timeout(ExampleCom) == Api.DefaultTimeout)
    assert(Conf.redirectPolicy(ExampleCom)(ExampleCom) == Api.PreventRedirectPolicy(ExampleCom))
    assert(Conf.robotsTxtPolicy(ExampleCom) == Api.RespectRobotsTxtPolicy)
  }

  it should "build the configuration with the correct URL-specific parameters" in {
    assert(Conf.fetcher(GoogleCom)(GoogleCom, Seq(Util.DummyHeader)) == GoogleComFetcher(GoogleCom, Seq(Util.DummyHeader)))
    assert(Conf.parser(GoogleCom)(GoogleComStr) == GoogleComParser(GoogleComStr))
    assert(Conf.delay(GoogleCom) == GoogleComDelay)
    assert(Conf.timeout(GoogleCom) == GoogleComTimeout)
    assert(Conf.redirectPolicy(GoogleCom)(GoogleCom) == GoogleComRedirectPolicy(GoogleCom))
    assert(Conf.robotsTxtPolicy(GoogleCom) == DoNotRespectRobotsTxtPolicy)
  }

  behavior of "Util"

  it should "make any url pas the check for the 'all' filter" in {
    assert(all()(FooNetUrl))
    assert(all()(BarComUrl))
  }

  it should "make only the specific host URLs pass the check for the 'host' filter" in {
    assert(host(FooNet)(FooNetUrl))
    assert(host(FooNet.toUpperCase)(FooNetUrl))
    assert(!host(BarCom)(FooNetUrl))
    assert(!host(BarCom.toUpperCase)(FooNetUrl))
  }

  it should "make only the specific URLs pass the check for the 'url' filter" in {
    assert(url(FooNetUrl.toString)(FooNetUrl))
    assert(url(FooNetUrl.toString.toUpperCase)(FooNetUrl))
    assert(!url(BarComUrl.toString)(FooNetUrl))
    assert(!url(BarComUrl.toString.toUpperCase)(FooNetUrl))
  }

  it should "modify headers list with the modify utility function" in {
    val actual = modify(BasicHttpHeader(HttpHeaders.USER_AGENT, "Modified"), BasicHttpHeader(HttpHeaders.CONTENT_LANGUAGE, "de-DE"))
      .apply(Seq(BasicHttpHeader(HttpHeaders.USER_AGENT, "Original"), BasicHttpHeader(HttpHeaders.CONTENT_LENGTH, "1024")))
      .toSet
    val expected = Seq(
      BasicHttpHeader(HttpHeaders.USER_AGENT, "Modified"),
      BasicHttpHeader(HttpHeaders.CONTENT_LANGUAGE, "de-DE"),
      BasicHttpHeader(HttpHeaders.CONTENT_LENGTH, "1024")
    ).toSet
    assert(actual == expected)
  }

  it should "add headers is no headers were initially specified with the modify utility function" in {
    val actual = modify(BasicHttpHeader(HttpHeaders.USER_AGENT, "Modified"), BasicHttpHeader(HttpHeaders.CONTENT_LANGUAGE, "de-DE"))
      .apply(Seq())
      .toSet
    val expected = Seq(
      BasicHttpHeader(HttpHeaders.USER_AGENT, "Modified"),
      BasicHttpHeader(HttpHeaders.CONTENT_LANGUAGE, "de-DE")
    ).toSet
    assert(actual == expected)
  }

  it should "overwrite original headers with the overwrite utility function" in {
    val actual = overwrite(BasicHttpHeader(HttpHeaders.USER_AGENT, "Replacer"))
      .apply(Seq(BasicHttpHeader(HttpHeaders.USER_AGENT, "Original"), BasicHttpHeader(HttpHeaders.CONTENT_LENGTH, "1024")))
    val expected = Seq(BasicHttpHeader(HttpHeaders.USER_AGENT, "Replacer"))
    assert(actual == expected)
  }

  it should "overwrite original empty list of headers with the overwrite utility function" in {
    val actual = overwrite(BasicHttpHeader(HttpHeaders.USER_AGENT, "Replacer")).apply(Seq.empty)
    val expected = Seq(BasicHttpHeader(HttpHeaders.USER_AGENT, "Replacer"))
    assert(actual == expected)
  }

  it should "make any url pas the check for the 'allow.all' filter" in {
    assert(allow.all()(FooNetUrl))
    assert(allow.all()(BarComUrl))
  }

  it should "make only the specific host URLs pass the check for the 'allow.host' filter" in {
    assert(allow.host(FooNet)(FooNetUrl))
    assert(allow.host(FooNet.toUpperCase)(FooNetUrl))
    assert(!allow.host(BarCom)(FooNetUrl))
    assert(!allow.host(BarCom.toUpperCase)(FooNetUrl))
  }

  it should "make only the specific URLs pass the check for the 'allow.url' filter" in {
    assert(allow.url(FooNetUrl.toString)(FooNetUrl))
    assert(allow.url(FooNetUrl.toString.toUpperCase)(FooNetUrl))
    assert(!allow.url(BarComUrl.toString)(FooNetUrl))
    assert(!allow.url(BarComUrl.toString.toUpperCase)(FooNetUrl))
  }

  it should "make no url pas the check for the 'disallow.all' filter" in {
    assert(!disallow.all()(FooNetUrl))
    assert(!disallow.all()(BarComUrl))
  }

  it should "make only the specific host URLs fail the check for the 'disallow.host' filter" in {
    assert(!disallow.host(FooNet)(FooNetUrl))
    assert(!disallow.host(FooNet.toUpperCase)(FooNetUrl))
    assert(disallow.host(BarCom)(FooNetUrl))
    assert(disallow.host(BarCom.toUpperCase)(FooNetUrl))
  }

  it should "make only the specific URLs fail the check for the 'disallow.url' filter" in {
    assert(!disallow.url(FooNetUrl.toString)(FooNetUrl))
    assert(!disallow.url(FooNetUrl.toString.toUpperCase)(FooNetUrl))
    assert(disallow.url(BarComUrl.toString)(FooNetUrl))
    assert(disallow.url(BarComUrl.toString.toUpperCase)(FooNetUrl))
  }

  it should "return the passed depth for all URLs for the follow.all filter" in {
    assert(follow.all(1)(FooNetUrl) == 1)
    assert(follow.all(1)(BarComUrl) == 1)
  }

  it should "throw an exception if the negative depth passed to the follow.all filter" in {
    assertThrows[IllegalArgumentException](
      follow.all(-1)(FooNetUrl)
    )
  }

  it should "return the passed depth only for specific host URLs for the follow.host filter" in {
    assert(follow.host(FooNet, 1)(FooNetUrl) == 1)
    assert(follow.host(FooNet.toLowerCase, 1)(FooNetUrl) == 1)
    assert(follow.host(FooNet, 1)(BarComUrl) == 0)
    assert(follow.host(FooNet.toLowerCase, 1)(BarComUrl) == 0)
  }

  it should "throw an exception if the negative depth passed the follow.host filter" in {
    assertThrows[IllegalArgumentException](
      follow.host(FooNet, -1)(FooNetUrl)
    )
  }

  it should "return the passed depth only for specific URLs for the follow.url filter" in {
    assert(follow.url(FooNetUrl.toString, 1)(FooNetUrl) == 1)
    assert(follow.url(FooNetUrl.toString.toLowerCase, 1)(FooNetUrl) == 1)
    assert(follow.url(FooNetUrl.toString, 1)(BarComUrl) == 0)
    assert(follow.url(FooNetUrl.toString.toLowerCase, 1)(BarComUrl) == 0)
  }

  it should "throw an exception if the negative depth passed the follow.url filter" in {
    assertThrows[IllegalArgumentException](
      follow.url(FooNetUrl.toString, -1)(FooNetUrl)
    )
  }

  it should "return zero depth for all URLs for the prevent.all filter" in {
    assert(prevent.all()(FooNetUrl) == 0)
    assert(prevent.all()(BarComUrl) == 0)
  }

  it should "return the passed depth only for specific host URLs for the prevent.host filter" in {
    assert(prevent.host(FooNet, 1)(FooNetUrl) == 0)
    assert(prevent.host(FooNet.toLowerCase, 1)(FooNetUrl) == 0)
    assert(prevent.host(FooNet, 1)(BarComUrl) == 1)
    assert(prevent.host(FooNet.toLowerCase, 1)(BarComUrl) == 1)
  }

  it should "throw an exception if the negative depth passed the prevent.host filter" in {
    assertThrows[IllegalArgumentException](
      prevent.host(FooNet, -1)(FooNetUrl)
    )
  }

  it should "return the passed depth only for specific URLs for the prevent.url filter" in {
    assert(prevent.url(FooNetUrl.toString, 1)(FooNetUrl) == 0)
    assert(prevent.url(FooNetUrl.toString.toLowerCase, 1)(FooNetUrl) == 0)
    assert(prevent.url(FooNetUrl.toString, 1)(BarComUrl) == 1)
    assert(prevent.url(FooNetUrl.toString.toLowerCase, 1)(BarComUrl) == 1)
  }

  it should "throw an exception if the negative depth passed the prevent.url filter" in {
    assertThrows[IllegalArgumentException](
      follow.url(FooNetUrl.toString, -1)(FooNetUrl)
    )
  }
}
