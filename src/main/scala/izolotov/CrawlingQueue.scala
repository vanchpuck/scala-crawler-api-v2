package izolotov

import izolotov.CrawlingQueue._

import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.locks.ReentrantLock

object CrawlingQueue {
  case class Item(baseUrl: String, url: String, depth: Int, queue: CrawlingQueue) {

//    def url(): String = {
//      url
//    }
//    def depth(): Int = depth
    def markAsInProgress(): Item = {
      queue.register()
      this
    }
    def markAsProcessed(): Item = {
      queue.deregister()
      this
    }
    def apply[A](f: String => A): A = {
      f.apply(url)
    }
  }
}

class CrawlingQueue(urls: Iterable[String]) extends Iterator[Item] {

  val urlsIterator: Iterator[String] = urls.iterator
  val buffer = new LinkedBlockingQueue[Item]()

  var counter: Int = 0
  val lock = new ReentrantLock()
  val condition = lock.newCondition()

  override def hasNext: Boolean = {
    lock.lock()
    try {
      if (urlsIterator.hasNext)
        true
      else if (buffer.peek() != null)
        true
      else {
        while (counter > 0) {
          condition.await()
          val hasNext = buffer.peek() != null || urlsIterator.hasNext
          if (hasNext)
            return true
        }
        false
      }
    }
    finally
      lock.unlock()
  }

  override def next(): Item = {
    lock.lock()
    try {
      if (buffer.peek() != null) buffer.poll() else { val n = urlsIterator.next(); Item(n, n, 0, this) }
    }
    finally
      lock.unlock()
  }

  def add(baseUrl :String, url: String, depth: Int): Unit = {
    lock.lock()
    try
      buffer.add(Item(baseUrl, url, depth, this))
    finally
      lock.unlock()
  }

  def register(): Unit = {
    lock.lock()
    try
      counter+=1
    finally
      lock.unlock()
  }

  def deregister(): Unit = {
    lock.lock()
    try {
      counter -= 1
      condition.signal()
    } finally
      lock.unlock()
  }
}
