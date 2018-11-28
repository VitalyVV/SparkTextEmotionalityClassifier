name := "bdproject"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"

libraryDependencies ++= Seq(
  //...
  "com.github.catalystcode" %% "streaming-rss-html" % "1.0.2",
  "org.jsoup" % "jsoup" % "1.10.3"
  //...
)