package com.example

import akka.actor.{ActorRef, ActorSystem, PoisonPill}
import Disruptor._
import PingActor._
import scala.concurrent.duration._
import akka.util.Timeout
import akka.pattern.{ask, pipe}

object ApplicationMain extends App {
  val system = ActorSystem("MyActorSystem")
  implicit val timeout = Timeout(60.seconds)
  import scala.concurrent.ExecutionContext.Implicits.global

  val random = new scala.util.Random

  val businessProcessor = system.actorOf(
    SampleBusinessProcessor.props,
    "businessProcessor")

  val futureDisruptor = businessProcessor ? BusinessProcessor.SubscribePublisher

  businessProcessor ! Disruptor.Initialized

  futureDisruptor onFailure { case error =>
    system.shutdown
  }

  futureDisruptor onSuccess { case disruptor: ActorRef =>
    val sampleClient = system.actorOf(
      SampleClient.props(businessProcessor),
      "sampleClient")
  }

  system.awaitTermination()
}
