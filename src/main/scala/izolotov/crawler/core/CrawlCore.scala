package izolotov.crawler.core

import izolotov.crawler.core.Api.Configuration

import java.net.URI
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

object CrawlCore {
  def apply[Raw, Doc](queue: CrawlQueue, conf: Configuration[Raw, Doc]): CrawlCore[Raw, Doc] = new CrawlCore(queue, conf)
}

class CrawlCore[Raw, Doc](queue: CrawlQueue, conf: Configuration[Raw, Doc]) {
  def foreach[Out](fn: Attempt[Doc] => Out)(implicit executionContext: ExecutionContext): Unit = {
    val extractor = new ParallelExtractor[Raw, Doc](conf)
    try {
      val futures = queue
        .map { item =>
          val future = extractor.extract(item.url).map { res =>
            val targetRedirect = res.redirectTarget.map { target =>
              Try(URI.create(target).toURL).foreach { t =>
                if (item.depth < conf.redirectPolicy(new URI(item.baseUrl).toURL)(t))
                  queue.add(target, item.url, item.depth + 1)
              }
              target
            }
            Attempt(res.url, res.doc, targetRedirect, Seq.empty)
          }
          future.onComplete {
            case Success(attempt) =>
              try
                fn(attempt)
              finally
                item.close()
          }
          future
        }
      Await.result(Future.sequence(futures), Duration.Inf)
    } finally {
      extractor.shutdown()
    }
  }
}
