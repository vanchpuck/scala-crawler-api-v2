package izolotov.crawler.core

import java.net.URL

object RobotRules {

  val Empty: EmptyRobotRules = EmptyRobotRules()

  private object EmptyRobotRules {
    def apply(): EmptyRobotRules = new EmptyRobotRules()
  }

  class EmptyRobotRules private () extends RobotRules {

    override def delay(): Option[Long] = None

    override def allowance(url: URL): Boolean = true
  }
}

trait RobotRules {
  def delay(): Option[Long]
  def allowance(url: URL): Boolean
}
