package com.example

import akka.actor.{ActorSystem, PoisonPill}
import Disruptor._
import PingActor._

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")

  val disruptor = system.actorOf(Disruptor.props(1024 * 1024, 100), "disruptor")
  val pingActor1 = system.actorOf(PingActor.props, "pingActor1")
  val pingActor2 = system.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = system.actorOf(PingActor.props, "pingActor3")
  val pongActor = system.actorOf(PongActor.props, "pongActor")

  disruptor ! Consumer(2, "/user/pingActor3")
  disruptor ! Consumer(1, "/user/pingActor1")
  disruptor ! Consumer(1, "/user/pingActor2")

  val random = new scala.util.Random

  disruptor ! Initialized
  for (i <- 0 until 100000) {
    disruptor.tell(Event(i.toString, PingMessage(i.toString)), pongActor)
  }
  disruptor.tell(Event("TERM", Terminate), pongActor)

  system.awaitTermination()
}
