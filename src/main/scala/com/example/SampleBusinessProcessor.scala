package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import akka.util.Timeout
import scala.concurrent.duration._

class SampleBusinessProcessor(bufSize: Int)
  extends BusinessProcessor(bufSize) with ActorLogging
{
  import Disruptor._
  import scala.concurrent.ExecutionContext.Implicits.global

  var msgCount = 0

  def journaler = new FileJournaler("/tmp/example.bin")

  type STATE = Int

  var state = 0

  val updateState: (AnyRef, Boolean) => AnyRef = (msg: AnyRef, replayed: Boolean) => {
    log.debug("SampleBusinessProcessor - updateState: {}, {}, {}",
      state, msg, replayed)
    state = state + 1
    s"Result: $state"
  }

  val pingActor2 = context.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = context.actorOf(PingActor.props, "pingActor3")

  disruptor ! Consumer(2, pingActor3.path.toString, 100)
  disruptor ! Consumer(1, pingActor2.path.toString, 100)

  def receiveCommand: Receive = {
    case msg: AnyRef =>
      //log.debug("SampleBusinessProcessor - received message: {}", msg)
      disruptor ! PersistentEvent(msg.toString, msg)
      //val sndr = sender
      //persist(msg) map {
        //case msg =>
          //sndr ! msg
          //msgCount += 1
      //}
  }
}

object SampleBusinessProcessor {
  val props = Props(new SampleBusinessProcessor(15))
}
