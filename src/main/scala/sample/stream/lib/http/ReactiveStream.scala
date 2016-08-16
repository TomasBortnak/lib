package sample.stream.lib.http

import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.{HttpResponse, HttpRequest}
import akka.util.ByteString
import com.redis._
import com.typesafe.config.ConfigFactory
import scala.util.{Failure, Success}

import scala.concurrent.Future
import scala.util.Success
import akka.stream.scaladsl._

/**
 * - should have some custom exception class
 */
class ReactiveStream {

  val host = ConfigFactory.load().getString("redis.host")
  val port = ConfigFactory.load().getString("redis.port")

  var redis = new RedisClientPool(host, port.toInt)
  var enableRecovery = true


  /**
   * By using HTTP get to send the message by reactive stream
   *
   * @param endpoint the address of the HTTP endpoint
   * @param queryString
   * @param message the message of the HTTP request data
   * @param identity the identity of the copy of data at redis which is key in redis
   */
  def get(endpoint: String, queryString: String, message: String, identity: String): Unit = {

    // what good delimer of it?
    save(identity, "GET" + ";" + endpoint + ";" + queryString + ";" + message)

    val connectionFlow: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
      Http().outgoingConnection(endpoint)

    val data = ByteString(message)

    val responseFuture: Future[HttpResponse] =
      Source.single(HttpRequest(GET, uri = queryString, entity = data))
        .via(connectionFlow)
        .runWith(Sink.head)

    responseFuture.andThen {
      case Success(_) => println("request succeded")
      case Failure(_) => println("request failed")
    }.andThen {
      case _ => system.terminate()
    }


  }

  /**
   * By using HTTP post to send the message by reactive stream
   *
   * @param endpoint the address of the HTTP endpoint
   * @param queryString
   * @param message the message of the HTTP request data
   * @param identity the identity of the copy of data at redis which is key in redis
   */
  def post(endpoint: String, queryString: String, message: String, identity: String) = {


    // what good delimer of it?
    save(identity, "POST" + ";" + endpoint + ";" + queryString + ";" + message)

    val connectionFlow: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
      Http().outgoingConnection(endpoint)

    val data = ByteString(message)

    val responseFuture: Future[HttpResponse] =
      Source.single(HttpRequest(POST, uri = queryString, entity = data))
        .via(connectionFlow)
        .runWith(Sink.head)

    responseFuture.andThen {
      case Success(_) => println("request succeded")
      case Failure(_) => {
        println("request failed")
      }
    }.andThen {
      case _ => system.terminate()
    }

  }

  /**
   * By retry the HTTP to send the message by reactive stream
   *
   * @param identity the identity of the copy of data at redis which is key in redis
   */
  def retry(identity: String) = {

    if (enableRecovery) {

      redis.withClient(
        client => {

          val content = client.get(identity).getOrElse(null)

          if (content != null) {

            val all = content.split(";")

            val method = all(0)
            val endpoint = all(1)
            val queryString = all(2)
            val message = all(3)

            method match {
              case "GET" => get(endpoint, queryString, message, identity)
              case "POST" => post(endpoint, queryString, message, identity)
            }

          }

        }
      )

    }

  }

  def save(identity: String, content: String) = {

    redis.withClient {
      client => {
        client.set(identity, content)
      }
    }

  }

}
