package com.example

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import Disruptor._
import PingActor._
import scala.concurrent.duration._
import akka.util.Timeout
import akka.pattern.ask

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")
  implicit val timeout = Timeout(60.seconds)
  import scala.concurrent.ExecutionContext.Implicits.global

  val random = new scala.util.Random

  val businessProcessor = system.actorOf(SampleBusinessProcessor.props, "businessProcessor")

  //journalActor ! JournalActor.Replay(businessProcessor, disruptor)

  //Thread.sleep(30000)
  val futureDisruptor = businessProcessor ? BusinessProcessor.SubscribePublisher

  businessProcessor ! Disruptor.Initialized

  futureDisruptor onFailure { case error =>
    system.shutdown
  }

  futureDisruptor onSuccess { case disruptor: ActorRef =>
    for (i <- 0 until 5000) {
      disruptor.tell(PersistentEvent(i.toString, PingMessage(i.toString)), businessProcessor)
    }
    disruptor.tell(PersistentEvent("TERM", Terminate), businessProcessor)
  }

  system.awaitTermination()
}
