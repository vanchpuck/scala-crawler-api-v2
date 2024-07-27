package izolotov.crawler.core

trait CrawlQueueHelper[A] {
  def queue(obj: A): CrawlQueue
}
