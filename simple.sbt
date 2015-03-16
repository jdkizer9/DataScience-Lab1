name := "Hello World!!"

version := "0.0.1"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"
libraryDependencies += "com.hadoop.gplcompression" % "hadoop-lzo" % "0.4.17"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
resolvers += "twttr.com" at "http://maven.twttr.com/"
