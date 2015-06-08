package com.example

import akka.actor.{Actor, ActorLogging, Props}

class PingActor extends Actor with ActorLogging {
  import PingActor._

  var counter = 0

  def receive = {
  	case Initialize =>
	    log.info("In PingActor - starting ping-pong")
  	case Disruptor.Process(index, id, data) =>
 	  log.info(s"In PingActor - received process message: $data")
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
