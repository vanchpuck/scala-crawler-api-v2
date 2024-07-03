package izolotov.crawler.core

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class DelayedApplier() {

  private var prevCallEndTime: Long = 0
  private val delayLock = new ReentrantLock()
  private val callerLock = new ReentrantLock()
  private val lockedForTimeout = delayLock.newCondition

  private class Delayer(val delay: Long) extends Runnable {
    override def run(): Unit = {
      delayLock.lock()
      try {
        Thread.sleep(delay)
      }
      finally {
        lockedForTimeout.signal()
        delayLock.unlock()
      }
    }
  }

  def apply[A, B](arg: A, fn: A => B, delayMillis: Long): B = {
    if (delayMillis < 0) throw new IllegalArgumentException("Delay must be non-negative")
    callerLock.lock()
    try {
      delayLock.lock()
      val callTime = System.currentTimeMillis
      val millisFromLastCall = callTime - prevCallEndTime
      if (millisFromLastCall < delayMillis) {
        val remainingDelay = delayMillis - millisFromLastCall
        new Thread(new Delayer(remainingDelay)).start()
        val executionStartTime = callTime + remainingDelay
        while (System.currentTimeMillis < executionStartTime) {
          lockedForTimeout.await(remainingDelay, TimeUnit.MILLISECONDS)
        }
      }
      try {
        fn.apply(arg)
      } finally {
        prevCallEndTime = System.currentTimeMillis
        delayLock.unlock()
      }
    } finally callerLock.unlock()
  }

}
