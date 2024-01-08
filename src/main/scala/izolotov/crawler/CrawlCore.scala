package izolotov.crawler

import izolotov.CrawlingQueue
import izolotov.crawler.SuperNewCrawlerApi.Configuration

import java.net.URI
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Success, Try}

object CrawlCore {
  def apply[Raw, Doc](queue: CrawlingQueue, conf: Configuration[Raw, Doc]): CrawlCore[Raw, Doc] = new CrawlCore(queue, conf)
}

class CrawlCore[Raw, Doc](queue: CrawlingQueue, conf: Configuration[Raw, Doc]) {
  def foreach[Out](fn: Attempt[Doc] => Out): Unit = {
    // FIXME create context instead of using the global one
    implicit val ec = ExecutionContext.global
    val extractor = new ParallelExtractor[Raw, Doc](conf)
    val futures = queue
      .map{
        item =>
          item.markAsInProgress()
          val future = extractor.extract(item.url)
            .map{
              res =>
                val targetRedirect = res.redirectTarget
                  .flatMap(target => Try(URI.create(target).toURL).toOption)
                  .filter(target => item.depth < conf.redirectPolicy(new URI(item.url).toURL)(target))
                  .map{target => queue.add(target.toString, item.depth + 1); target}
                  .map(u => u.toString)
                println(targetRedirect)
                Attempt(res.url, res.doc, targetRedirect, Seq.empty)
            }
          future.onComplete {
            case Success(attempt) =>
              try
                fn(attempt)
              finally
                item.markAsProcessed()
          }
          future
      }
    Await.result(Future.sequence(futures), Duration.Inf)
  }
}
