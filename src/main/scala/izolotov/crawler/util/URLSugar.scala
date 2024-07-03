package izolotov.crawler.util

import java.net.URL

object URLSugar {

  val DefaultDepth: Int = 5

  def all(): URL => Boolean = _ => true

  def host(host: String): URL => Boolean = url => url.getHost.toLowerCase == host.toLowerCase

  def url(url: String): URL => Boolean = _url => _url.toString.toLowerCase == url.toLowerCase

  def allow: URLAllowancePolicy.type = URLAllowancePolicy

  def disallow: URLDisallowancePolicy.type = URLDisallowancePolicy

  def follow: RedirectTrackingPolicy.type = RedirectTrackingPolicy

  def prevent: RedirectPreventingPolicy.type = RedirectPreventingPolicy

  // TODO good to make it private
  protected object URLAllowancePolicy {
    def all(): URL => Boolean = _ => true

    def host(host: String): URL => Boolean = url => url.getHost.toLowerCase == host.toLowerCase

    def url(url: String): URL => Boolean = _url => _url.toString.toLowerCase == url.toLowerCase
  }

  protected object URLDisallowancePolicy {
    def all(): URL => Boolean = _ => false

    def host(host: String): URL => Boolean = url => url.getHost.toLowerCase != host.toLowerCase

    def url(url: String): URL => Boolean = _url => _url.toString.toLowerCase != url.toLowerCase
  }

  protected object RedirectTrackingPolicy {

    def all(depth: Int = DefaultDepth): URL => Int = {
      require(depth >= 0)
      _ => depth
    }

    def host(host: String, depth: Int = DefaultDepth): URL => Int = {
      require(depth >= 0)
      _url => if (_url.getHost.toLowerCase == host.toLowerCase) depth else 0
    }

    def url(url: String, depth: Int = DefaultDepth): URL => Int = {
      require(depth >= 0)
      _url => if (_url.toString.toLowerCase == url.toLowerCase) depth else 0
    }
  }

  protected object RedirectPreventingPolicy {
    def all(): URL => Int = _ => 0

    def host(host: String, depth: Int = DefaultDepth): URL => Int = {
      require(depth >= 0)
      _url => if (_url.getHost.toLowerCase != host.toLowerCase) depth else 0
    }

    def url(url: String, depth: Int = DefaultDepth): URL => Int = {
      require(depth >= 0)
      _url => if (_url.toString.toLowerCase != url.toLowerCase) depth else 0
    }
  }

}
