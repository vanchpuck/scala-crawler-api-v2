package izolotov.crawler.util

import izolotov.crawler.core.CrawlQueueHelper
import izolotov.{BoundedCrawlQueue, CrawlQueue}

object CrawlQueueHelpers {
  implicit object SeqHelper extends CrawlQueueHelper[Seq[String]]{
    override def queue(obj: Seq[String]): CrawlQueue = {
      new BoundedCrawlQueue(obj.iterator)
    }
  }
}
