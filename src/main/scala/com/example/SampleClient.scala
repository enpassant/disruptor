package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

class SampleClient(businessProcessor: ActorRef) extends Actor with ActorLogging
{
  import SampleClient._
  import Disruptor._

  val random = new scala.util.Random
  var counter = 0

  log.info("SampleClient - send events")

  for (i <- 0 until 20) {
    businessProcessor ! PingMessage(i.toString)
  }
  businessProcessor ! Terminate

  def receive = {
    case Initialize =>
      log.info("SampleClient - starting ping-pong")

    case Disruptor.Processed(index, "Terminate", Terminate) =>
      log.info("SampleClient - TERMINATED. Processed: {}, {}",
        index, counter)
      context.system.shutdown

    case msg =>
      log.debug("SampleClient - received message: {}", msg)
  }
}

case class PingMessage(text: String)

object SampleClient {
  def props(businessProcessor: ActorRef) = Props(
    new SampleClient(businessProcessor))
  case object Initialize
}
