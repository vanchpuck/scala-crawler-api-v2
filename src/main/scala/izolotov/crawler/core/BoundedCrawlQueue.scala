package izolotov.crawler.core

import BoundedCrawlQueue._

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

object BoundedCrawlQueue {

  def apply(urls: Iterator[String]): BoundedCrawlQueue = new BoundedCrawlQueue(urls)

  class LQItem private[BoundedCrawlQueue](val url: String, val baseUrl: String, val depth: Int, q: BoundedCrawlQueue) extends CrawlQueue.Item {
    q.registerProcessingItem()
    def close(): Unit = {
      q.deregisterProcessingItem()
    }
  }
}

class BoundedCrawlQueue(urls: Iterator[String]) extends CrawlQueue {

  private val buffer = new LinkedBlockingQueue[LQItem]()

  private var counter: Int = 0
  private val lock = new ReentrantLock()
  private val condition = lock.newCondition()

  override def hasNext: Boolean = {
    lock.lock()
    try {
      if (urls.hasNext)
        true
      else if (buffer.peek() != null)
        true
      else {
        while (counter > 0) {
          condition.await()
          val hasNext = buffer.peek() != null || urls.hasNext
          if (hasNext)
            return true
        }
        false
      }
    }
    finally
      lock.unlock()
  }

  override def next(): LQItem = {
    lock.lock()
    try {
      if (buffer.peek() != null) buffer.poll() else { val next = urls.next(); new LQItem(next, next, 0, this) }
    }
    finally
      lock.unlock()
  }

  def add(url: String, baseUrl: String, depth: Int): Unit = {
    lock.lock()
    try
      buffer.add(new LQItem(url, baseUrl, depth, this))
    finally
      lock.unlock()
  }

  private def registerProcessingItem(): Unit = {
    lock.lock()
    try
      counter+=1
    finally
      lock.unlock()
  }

  private def deregisterProcessingItem(): Unit = {
    lock.lock()
    try {
      counter -= 1
      condition.signal()
    } finally
      lock.unlock()
  }
}
