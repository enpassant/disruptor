package com.example

import akka.actor.{ActorSystem, PoisonPill}
import Disruptor._
import PingActor._

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")

  val disruptor = system.actorOf(Disruptor.props(160), "disruptor")
  val pingActor1 = system.actorOf(PingActor.props, "pingActor1")
  val pingActor2 = system.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = system.actorOf(PingActor.props, "pingActor3")
  val pongActor = system.actorOf(PongActor.props, "pongActor")

  disruptor ! Consumer(1, "/user/pingActor3")
  disruptor ! Consumer(1, "/user/pingActor1")
  disruptor ! Consumer(1, "/user/pingActor2")
  disruptor ! Consumer(2, "/user/pongActor")

  val random = new scala.util.Random

  disruptor ! Initialized
  for (i <- 0 until 200) {
    disruptor ! Event(i.toString, PingMessage(i.toString))
    Thread.sleep(random.nextInt % 50 + 50)
  }

  Thread.sleep(15000)

  system.shutdown()
  system.awaitTermination()
}
