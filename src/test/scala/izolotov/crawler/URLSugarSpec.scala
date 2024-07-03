package izolotov.crawler

import org.scalatest.flatspec.AnyFlatSpec
import izolotov.crawler.util.URLSugar._

import java.net.{URI, URL}

import URLSugarSpec._

object URLSugarSpec {
  val FooNet = "Foo.net"
  val BarCom = "Bxar.com"
  val FooNetUrl: URL = URI.create(s"http://$FooNet").toURL
  val BarComUrl: URL = URI.create(s"http://$BarCom").toURL
}

class URLSugarSpec extends AnyFlatSpec {

  behavior of "URLSugar"

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

  behavior of "URLAllowancePolicy"

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

  behavior of "URLDisallowancePolicy"

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

  behavior of "RedirectTrackingPolicy"

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

  behavior of "RedirectPreventingPolicy"

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
