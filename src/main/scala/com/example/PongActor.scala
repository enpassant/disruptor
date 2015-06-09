package com.example

import akka.actor.{Actor, ActorLogging, Props}

class PongActor extends Actor with ActorLogging {
  import PongActor._

  val random = new scala.util.Random
  var counter = 0

  def receive = {
  	case Disruptor.Process(index, id, data) =>
          counter += 1
 	  log.info(s"In PongActor - received process message: $counter")
          Thread.sleep(random.nextInt % 10 + 10)
          sender ! Disruptor.Processed(index, id)
  }
}

object PongActor {
  val props = Props[PongActor]
  case class PongMessage(text: String)
}
