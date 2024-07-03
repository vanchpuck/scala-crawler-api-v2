package izolotov.crawler.core

trait HttpHeader {

  def name(): String

  def value(): String

}
