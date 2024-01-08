package izolotov.app

import com.google.common.collect.Lists
import crawlercommons.robots.SimpleRobotRulesParser

import java.nio.file.{Files, Paths}

//import scala.sys.process.processInternal.URL

object Robots {

  def main(args: Array[String]): Unit = {
    val parser = new SimpleRobotRulesParser()

    val rules = parser.parseContent(
      "https://en.wikipedia.org/robots.txt",
      Files.readAllBytes(Paths.get("/Users/izolotov/IdeaProjects/ScalaTest/src/main/scala/izolotov/app/robots1.txt")),
      "text/plain",
      Lists.newArrayList("bot")
    )
    println(rules.isAllowed("https://en.wikipedia.org/abc?acti=1"))
    println(rules)
//    println(rules.getCrawlDelay)
//    println(rules.getRobotRules.get(0).getPrefix)
//    println(rules.getRobotRules.get(0).isAllow)

//    Seq(1).map{a => println(a); 1}
//    val a = Map("1" -> "2", "3" -> "4").map(entry => s"${entry._1}=${entry._2}").mkString("; ")
//    println(a)

//    val url = new URL("http://example.com")
//    println(s"${url.getProtocol}://${url.getHost}/robots.txt")

  }
}
