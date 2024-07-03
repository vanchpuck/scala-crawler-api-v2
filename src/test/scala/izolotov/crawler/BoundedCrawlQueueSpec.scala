package izolotov.crawler

import izolotov.BoundedCrawlQueue
import org.scalatest.flatspec.AnyFlatSpec

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import BoundedCrawlQueueSpec._

import scala.util.Using

object BoundedCrawlQueueSpec {
  val Url1 = "http://example.com/1"
  val Url2 = "http://example.com/2"
}

class BoundedCrawlQueueSpec extends AnyFlatSpec {

  implicit val ec: ExecutionContextExecutor = ExecutionContext.global

  behavior of "BoundedCrawlQueue"

  it should "indicate that there is no next item if the iterator is empty" in {
    val iterator = BoundedCrawlQueue(Seq[String]().iterator)
    assert(!iterator.hasNext)
  }

  it should "wait before the last item is processed before indicating that iterator is empty" in {
    val iterator = BoundedCrawlQueue(Seq(Url1).iterator)
    val item = iterator.next()
    val future = Future {
      Thread.sleep(10L)
      item.close()
      System.currentTimeMillis()
    }
    iterator.hasNext
    assert(System.currentTimeMillis() >= Await.result(future, Duration.Inf))
  }

  it should "follow the common iterator pattern principles" in {
    val iterator = BoundedCrawlQueue(Seq(Url1, Url2).iterator)
    assert(iterator.hasNext)
    assert(iterator.hasNext)
    assert(Using(iterator.next())(item => item.url == Url1).get)
    assert(iterator.hasNext)
    assert(Using(iterator.next())(item => item.url == Url2).get)
    assert(!iterator.hasNext)
    assertThrows[NoSuchElementException](
      iterator.next()
    )
  }

  it should "return newly added items firstly" in {
    val iterator = BoundedCrawlQueue(Seq(Url1).iterator)
    iterator.add(Url2, Url2, 0)
    assert(iterator.next().url == Url2)
    assert(iterator.next().url == Url1)
  }

  it should "return items with correct url, baseUrl and depth for newly added items" in {
    val iterator = BoundedCrawlQueue(Seq[String]().iterator)
    iterator.add(Url1, Url2, 1)
    val next = iterator.next()
    assert(next.url == Url1)
    assert(next.baseUrl == Url2)
    assert(next.depth == 1)
  }

}
