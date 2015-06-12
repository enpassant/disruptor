package com.example

import akka.actor.{ActorSystem, PoisonPill}
import Disruptor._
import PingActor._

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")

  val random = new scala.util.Random

  val disruptor = system.actorOf(Disruptor.props(1024 * 1024, 100), "disruptor")
  val journalActor = system.actorOf(JournalActor.props, "journalActor")
  val pingActor2 = system.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = system.actorOf(PingActor.props, "pingActor3")
  val pongActor = system.actorOf(PongActor.props, "pongActor")

  disruptor ! Consumer(2, "/user/pingActor3")
  disruptor ! Consumer(1, "/user/journalActor")
  disruptor ! Consumer(1, "/user/pingActor2")

  disruptor ! Initialized

  journalActor ! JournalActor.Replay(pongActor, disruptor)

  Thread.sleep(30000)

  for (i <- 0 until 1000000) {
    disruptor.tell(Event(i.toString, PingMessage(i.toString)), pongActor)
  }
  disruptor.tell(Event("TERM", Terminate), pongActor)

  system.awaitTermination()
}
