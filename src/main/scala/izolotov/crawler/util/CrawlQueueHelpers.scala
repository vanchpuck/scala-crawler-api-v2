package izolotov.crawler.util

import izolotov.crawler.core.{BoundedCrawlQueue, CrawlQueue, CrawlQueueHelper}

object CrawlQueueHelpers {
  implicit object SeqHelper extends CrawlQueueHelper[Seq[String]]{
    override def queue(obj: Seq[String]): CrawlQueue = {
      new BoundedCrawlQueue(obj.iterator)
    }
  }
}
