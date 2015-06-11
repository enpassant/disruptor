package com.example

import akka.actor.{Actor, ActorLogging, Props}

class PongActor extends Actor with ActorLogging {
  import PongActor._

  val random = new scala.util.Random
  var counter = 0

  def receive = {
    case Disruptor.Processed(index, "TERM") =>
      log.info(s"In PongActor - TERMINATED. Processed: $index, $counter")
      context.system.shutdown

    case Disruptor.Processed(index, data) =>
      counter += 1
      log.debug(s"In PongActor - received process message: $data, $counter")
  }
}

object PongActor {
  val props = Props[PongActor]
  case class PongMessage(text: String)
}
