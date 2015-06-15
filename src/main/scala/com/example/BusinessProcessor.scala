package com.example

import akka.actor.{Actor, ActorLogging, Props}

class BusinessProcessor extends Actor with ActorLogging {
  import BusinessProcessor._

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

object BusinessProcessor {
  val props = Props[BusinessProcessor]
  case class PongMessage(text: String)
}
