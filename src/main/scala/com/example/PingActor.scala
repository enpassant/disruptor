package com.example

import akka.actor.{Actor, ActorLogging, Props}

class PingActor extends Actor with ActorLogging {
  import PingActor._

  val random = new scala.util.Random
  var counter = 0

  def receive = {
  	case Initialize =>
	    log.info("In PingActor - starting ping-pong")
  	case Disruptor.Process(index, id, data) =>
          counter += 1
// 	  log.info(s"In PingActor - received process message: $counter")
          Thread.sleep(random.nextInt % 10 + 10)
          sender ! Disruptor.Processed(index, id)
  	case msg =>
  	  log.info(s"In PingActor - received message: $msg")
  }
}

object PingActor {
  val props = Props[PingActor]
  case object Initialize
  case class PingMessage(text: String)
}
