package com.example

import akka.actor.{ActorSystem, PoisonPill}
import Disruptor._
import PingActor._

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")

  val disruptor = system.actorOf(Disruptor.props(16), "disruptor")
  val pingActor1 = system.actorOf(PingActor.props, "pingActor1")
  val pingActor2 = system.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = system.actorOf(PingActor.props, "pingActor3")
  val pongActor = system.actorOf(PongActor.props, "pongActor")

  disruptor ! Consumer(1, "/user/pingActor3")
  disruptor ! Consumer(1, "/user/pingActor1")
  disruptor ! Consumer(1, "/user/pingActor2")
  disruptor ! Consumer(2, "/user/pongActor")

  disruptor ! Initialized
  disruptor ! Event(PingMessage("ping1"))
  disruptor ! Event(PingMessage("ping2"))
  disruptor ! Event(PingMessage("ping3"))
  disruptor ! Event(PingMessage("ping4"))
  disruptor ! Event(PingMessage("ping5"))
  disruptor ! Event(PingMessage("ping6"))
  disruptor ! Event(PingMessage("ping7"))

  Thread.sleep(3000)

  system.shutdown()
  system.awaitTermination()
}
