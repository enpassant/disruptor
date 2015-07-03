package com.example

import akka.actor.{Actor, ActorLogging, Props}

class PingActor extends Actor with ActorLogging {
  import PingActor._

  val random = new scala.util.Random
  var counter = 0

  def receive = {
    case Initialize =>
      log.debug("In PingActor - starting ping-pong")
    case Disruptor.Process(seqNr, index, id, data: Seq[AnyRef]) =>
      counter += 1
      sender ! Disruptor.Processed(index, id, data)

    case Disruptor.Process(seqNr, index, id, data) =>
      counter += 1
      sender ! Disruptor.Processed(index, id, data)

    case msg =>
      log.debug(s"In PingActor - received message: $msg")
  }
}

object PingActor {
  val props = Props[PingActor]
  case object Initialize
  case class PingMessage(text: String)
}
