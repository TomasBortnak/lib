package db

import sample.stream.service.WordSqlService

import collection.mutable.Stack
import org.scalatest._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ExampleSpec extends FlatSpec with Matchers {

  "A Stack" should "pop values in last-in-first-out order" in {

    //WordFinder.compareWith("hello")
    Await.result(WordSqlService.compareWith("hello"), Duration("5 seconds"))

  }

  it should "throw NoSuchElementException if an empty stack is popped" in {

  }
}