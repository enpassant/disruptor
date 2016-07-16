package com.example

import akka.actor.{Actor, ActorLogging, Props}

class PingActor extends Actor with ActorLogging {
  import PingActor._
  import Disruptor._

  val random = new scala.util.Random
  var counter = 0

  def receive = {
    case Initialize =>
      log.debug("PingActor - starting ping-pong")

    case processValue @ Disruptor.Process(seqNr, index, id, data: Seq[AnyRef]) =>
      log.debug("PingActor - Process: {}", processValue)
      counter += 1
      sender ! Disruptor.Processed(index, id, None)

    case processValue @ Disruptor.Process(seqNr, index, id, data) =>
      log.debug("PingActor - Process: {}", processValue)
      counter += 1
      sender ! Disruptor.Processed(index, id, None)

    case msg =>
      log.debug("PingActor - received message: {}", msg)
  }
}

object PingActor {
  val props = Props[PingActor]
  case object Initialize
  case class PingMessage(text: String)
}
