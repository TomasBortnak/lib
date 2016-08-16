import scalariform.formatter.preferences._

name := """stream"""

version := "1.1"

scalaVersion := "2.11.7"

// https://github.com/dnvriend/akka-http-test/blob/master/build.sbt
libraryDependencies ++= {
  val akkaVer = "2.4.2"
  Seq(
    "com.typesafe.akka" % "akka-stream_2.11" % akkaVer,
    "com.typesafe.akka" %% "akka-slf4j" % akkaVer,
    "com.typesafe.akka" %% "akka-http-experimental" % akkaVer,
    "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaVer,
    "com.typesafe.akka" %% "akka-http-xml-experimental" % akkaVer,
    "com.typesafe.akka" % "akka-actor_2.11" % akkaVer,
    "org.scalactic" %% "scalactic" % "2.2.6",
    "org.scalatest" %% "scalatest" % "2.2.6"
  )
}

libraryDependencies ++= Seq(
  "com.typesafe.slick" %% "slick-hikaricp" % "3.1.1",
  "org.slf4j" % "slf4j-nop" % "1.6.4",
  "mysql" % "mysql-connector-java" % "5.1.36"
)

//no reactive redis client
//libraryDependencies += "com.github.etaty" %% "rediscala" % "1.6.0"


libraryDependencies ++= Seq(
  "net.debasishg" %% "redisclient" % "3.0"
)

// https://github.com/spray/spray-template/blob/on_spray-can_1.3_scala-2.11/build.sbt
/*
libraryDependencies ++= {
  val sprayVer = "1.3.3"
  Seq(
    "io.spray"            %%  "spray-can"     % sprayVer,
    "io.spray"            %%  "spray-routing" % sprayVer
  )
}
*/
