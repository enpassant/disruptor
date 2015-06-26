package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import scala.concurrent.duration._

class BusinessProcessor extends Actor with ActorLogging {
  import JournalActor._
  import BusinessProcessor._
  import Disruptor._

  val random = new scala.util.Random
  var counter = 0
  var publishers = List.empty[ActorRef]
  var replaying = true

  val disruptor = context.actorOf(Disruptor.props(BufSize), "disruptor")
  val journalActor = context.actorOf(JournalActor.props, "journalActor")
  val pingActor2 = context.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = context.actorOf(PingActor.props, "pingActor3")

  disruptor ! Consumer(2, pingActor3.path.toString, 100)
  disruptor ! Consumer(1, journalActor.path.toString, 100)
  disruptor ! Consumer(1, pingActor2.path.toString, 100)
  disruptor ! Consumer(3, self.path.toString, 100)

  disruptor ! Initialized

  def receive = {
    case Initialized =>
      log.info(s"BusinessProcessor - Start replaying")
      journalActor ! Replay(self, disruptor, BufSize - 10)

    case SubscribePublisher =>
      publishers = publishers ++ List(sender)

    case ReceiveTimeout =>
      log.info(s"BusinessProcessor - Start publishing")
      publishers foreach { _ ! disruptor }
      context.setReceiveTimeout(Duration.Undefined)

    case ReplayFinished =>
      replaying = false
      context.setReceiveTimeout(1.second)

    case Disruptor.Process(seqNr, index, id, data) =>
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Processed(index, "TERM") =>
      log.info(s"In PongActor - TERMINATED. Processed: $index, $counter")
      context.system.shutdown

    case Disruptor.Processed(index, data) =>
      counter += 1
      log.debug(s"In PongActor - received process message: $index, $counter, $data")
  }
}

object BusinessProcessor {
  val BufSize = 1024 * 1024
  val props = Props[BusinessProcessor]

  case object SubscribePublisher
  case class PongMessage(text: String)
}
