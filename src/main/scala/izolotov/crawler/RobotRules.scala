package izolotov.crawler

import java.net.URL

object RobotRules {
  def empty(): RobotRules = {
    new EmptyRobotRules()
  }

  private class EmptyRobotRules extends RobotRules {

    override def delay(): Option[Long] = None

    override def allowance(url: URL): Boolean = true
  }
}

trait RobotRules {
  def delay(): Option[Long]
  def allowance(url: URL): Boolean
}
