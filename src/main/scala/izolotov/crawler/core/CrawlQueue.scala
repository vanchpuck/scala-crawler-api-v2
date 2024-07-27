package izolotov.crawler.core

object CrawlQueue {
  trait Item extends AutoCloseable{
    def baseUrl: String
    def url: String
    def depth: Int
  }
}

trait CrawlQueue extends Iterator[CrawlQueue.Item]{
  def add(url: String, baseUrl: String, depth: Int): Unit
}
