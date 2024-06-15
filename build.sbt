name := "scala-crawler-api"

version := "0.1"

scalaVersion := "2.13.12"

// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "33.2.0-jre"
// https://mvnrepository.com/artifact/org.jsoup/jsoup
libraryDependencies += "org.jsoup" % "jsoup" % "1.17.2"
// https://mvnrepository.com/artifact/com.github.crawler-commons/crawler-commons
libraryDependencies += "com.github.crawler-commons" % "crawler-commons" % "1.4"
// https://mvnrepository.com/artifact/org.apache.httpcomponents.core5/httpcore5
libraryDependencies += "org.apache.httpcomponents.core5" % "httpcore5" % "5.2.4"
//// https://mvnrepository.com/artifact/net.sourceforge.htmlunit/htmlunit
//libraryDependencies += "net.sourceforge.htmlunit" % "htmlunit" % "2.70.0"

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % Test
// https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-server
libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "12.0.8" % Test
// https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-util
libraryDependencies += "org.eclipse.jetty" % "jetty-util" % "12.0.8" % Test
// https://mvnrepository.com/artifact/org.eclipse.jetty/jetty-server
libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "12.0.8" % Test
// https://mvnrepository.com/artifact/org.json4s/json4s-native
libraryDependencies += "org.json4s" %% "json4s-native" % "4.1.0-M5" % Test

