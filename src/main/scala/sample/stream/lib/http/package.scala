package sample.stream.lib

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

package object http {

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher
}
