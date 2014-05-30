name := "RDDHelper"

version := "1.0"

resolvers += "MapR Repository" at "http://repository.mapr.com/maven/"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.1"

libraryDependencies += "org.apache.hadoop" % "hadoop-core" % "1.0.3-mapr-2.1.3.1"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
    