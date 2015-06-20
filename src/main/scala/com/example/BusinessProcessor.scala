package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props}

class BusinessProcessor extends Actor with ActorLogging {
  import JournalActor._
  import BusinessProcessor._
  import Disruptor._

  val random = new scala.util.Random
  var counter = 0
  var publishers = List.empty[ActorRef]
  var replaying = true 

  val disruptor = context.actorOf(Disruptor.props(1024 * 1024, 100), "disruptor")
  val journalActor = context.actorOf(JournalActor.props, "journalActor")
  val pingActor2 = context.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = context.actorOf(PingActor.props, "pingActor3")

  def getPath(actorRef: ActorRef) = "/" + (actorRef.path.elements mkString "/")

  disruptor ! Consumer(2, getPath(pingActor3))
  disruptor ! Consumer(1, getPath(journalActor))
  disruptor ! Consumer(1, getPath(pingActor2))

  disruptor ! Initialized

  def receive = {
    case Initialized =>
      log.info(s"BusinessProcessor - Start replaying")
      journalActor ! Replay(self, disruptor)

    case SubscribePublisher =>
      publishers = publishers ++ List(sender)

    case ReplayFinished =>
      log.info(s"BusinessProcessor - Start publishing")
      replaying = false
      publishers foreach { _ ! disruptor }

    case Disruptor.Processed(index, "TERM") =>
      log.info(s"In PongActor - TERMINATED. Processed: $index, $counter")
      context.system.shutdown

    case Disruptor.Processed(index, data) =>
      counter += 1
      if (replaying) journalActor ! ReplayNext(self, disruptor)
      log.debug(s"In PongActor - received process message: $index, $counter, $data")
  }
}

object BusinessProcessor {
  val props = Props[BusinessProcessor]

  case object SubscribePublisher
  case class PongMessage(text: String)
}
