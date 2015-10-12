package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

class SampleClient(businessProcessor: ActorRef) extends Actor with ActorLogging {
  import SampleClient._
  import Disruptor._

  val random = new scala.util.Random
  var counter = 0

  log.info(s"In SampleClient - send events")

  for (i <- 0 until 5) {
    businessProcessor ! PingMessage(i.toString)
  }
  businessProcessor ! Terminate

  def receive = {
    case Initialize =>
      log.info("In SampleClient - starting ping-pong")

    case Disruptor.Processed(index, "Terminate", Terminate) =>
      log.info(s"In SampleClient - TERMINATED. Processed: $index, $counter")
      context.system.shutdown

    case msg =>
      log.info(s"In SampleClient - received message: $msg")
  }
}

object SampleClient {
  def props(businessProcessor: ActorRef) = Props(new SampleClient(businessProcessor))
  case object Initialize
  case class PingMessage(text: String)
}
