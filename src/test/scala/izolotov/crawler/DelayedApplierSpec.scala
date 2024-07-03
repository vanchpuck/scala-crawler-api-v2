package izolotov.crawler

import izolotov.crawler.core.DelayedApplier
import org.scalatest.flatspec.AnyFlatSpec

class DelayedApplierSpec extends AnyFlatSpec {

  behavior of "DelayedApplierSpec"

  it should "keep the delay between the subsequent invocations" in {
    val f: Unit => Long = _ => System.currentTimeMillis()
    val expectedDelay = 1000L
    val applier = new DelayedApplier()
    val minActualDelay = Seq.fill(3)(applier.apply((), f, expectedDelay)).sliding(2).map(seq => seq(1) - seq.head).min
    println(minActualDelay)
    assert(minActualDelay >= expectedDelay)
  }

  it should "allow to pass only non-negative delay value" in {
    assertThrows[IllegalArgumentException](
      new DelayedApplier().apply((), println, -1)
    )
  }

}
