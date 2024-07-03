package izolotov.crawler.core

import izolotov.CrawlQueue

trait CrawlQueueHelper[A] {
  def queue(obj: A): CrawlQueue
}
