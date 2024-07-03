package izolotov.crawler

import izolotov.crawler.core.Api.ConfigurationBuilder
import org.scalatest.flatspec.AnyFlatSpec
import ApiSpec._
import izolotov.crawler.core.{Api, HttpHeader}
import izolotov.crawler.util.URLSugar

import java.net.{URI, URL}

object ApiSpec {

  val Parallelism: Int = 1
  val Redirect: String => Option[String] = _ => None

  val DefaultFetcherOutput = "Default fetcher output"
  val DefaultParserOutput = "Default parser output"

  private val ExampleCom: URL = URI.create("http://example.com").toURL
  private val ExampleComStr = ExampleCom.toString
  private val ExampleComFetcher: (URL, Seq[HttpHeader]) => String = (a, _) => a.toString
  private val ExampleComParser: String => String = raw => raw

  private val GoogleCom: URL = URI.create("http://google.com").toURL
  private val GoogleComStr = GoogleCom.toString
  private val GoogleComPredicate: URL => Boolean = URLSugar.url(GoogleCom.toString)
  private val GoogleComFetcher: (URL, Seq[HttpHeader]) => String = (a, _) => a.toString
  private val GoogleComParser: String => String = raw => raw
  private val GoogleComDelay = 1L
  private val GoogleComTimeout = 1L
  private val GoogleComRedirectPolicy: URL => Int = _ => 1

  private val DoNotRespectRobotsTxtPolicy = false

  private val Conf = new ConfigurationBuilder[String ,String](
    parallelism = Parallelism,
    redirect = Redirect,
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
}
