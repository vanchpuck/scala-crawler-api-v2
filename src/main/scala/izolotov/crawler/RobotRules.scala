package izolotov.crawler

import java.net.URL

object RobotRules {
  def empty(): EmptyRobotRules = {
    new EmptyRobotRules()
  }


  class EmptyRobotRules extends RobotRules {

    override def delay(): Option[Long] = None

    override def allowance(url: URL): Boolean = true
  }

//  case object A extends RobotRules {
//
//    override def delay(): Option[Long] = ???
//
//    override def allowance(url: URL): Boolean = ???
//  }

  object Stub extends EmptyRobotRules

}

trait RobotRules {
  def delay(): Option[Long]
  def allowance(url: URL): Boolean
}
