package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent._

abstract class BusinessProcessor(bufSize: Int)
  extends Actor with ActorLogging
{
  import JournalActor._
  import BusinessProcessor._
  import Disruptor._

  implicit val timeout = Timeout(60.seconds)
  import scala.concurrent.ExecutionContext.Implicits.global

  val random = new scala.util.Random
  var counter = 0
  var publishers = List.empty[ActorRef]
  var replaying = true

  def journaler: Journaler

  val disruptor = context.actorOf(Disruptor.props(BufSize), "disruptor")
  val journalActor = context.actorOf(JournalActor.props(journaler), "journalActor")
  val pingActor2 = context.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = context.actorOf(PingActor.props, "pingActor3")

  disruptor ! Consumer(2, pingActor3.path.toString, 100)
  disruptor ! Consumer(1, journalActor.path.toString, 100)
  disruptor ! Consumer(1, pingActor2.path.toString, 100)
  disruptor ! Consumer(3, self.path.toString, 100)

  disruptor ! Initialized

  def persist[T](msg: AnyRef): Future[Any] = {
    disruptor ? PersistentEvent(msg.toString, msg)
  }

  def receiveCommand: Receive

  def receiveRecover: Receive

//  def persist(event: AnyRef)(handler: AnyRef => Unit): Unit = {
//    disruptor ! PersistentEvent(counter.toString, event)
//  }

  def receive = replay orElse process

  def replay: Receive = {
    case Initialized =>
      log.info(s"BusinessProcessor - Start replaying")
      journalActor ! Replay(self, disruptor, bufSize - 10)

    case SubscribePublisher =>
      publishers = publishers ++ List(sender)

    case Disruptor.Process(0, index, id, command) =>
      command match {
        case ReplayFinished =>
          log.info(s"BusinessProcessor - Start publishing")
          publishers foreach { _ ! disruptor }
          sender ! Disruptor.Processed(index, id, command)
          context.become(process orElse receiveCommand)
      }
  }

  def process: Receive = {
    case Disruptor.Process(seqNr, index, id, data) =>
      data match {
        case Array(seq @ _*) => seq foreach receiveRecover
        case _ => receiveRecover(data)
      }
      sender ! Disruptor.Processed(index, id, data)

    case Disruptor.Processed(index, "Terminate", Terminate) =>
      log.info(s"In PongActor - TERMINATED. Processed: $index, $counter")
      context.system.shutdown

    case Disruptor.Processed(index, id, data) =>
      counter += 1
      log.debug(s"In PongActor - received process message: $index, $counter, $data")
  }
}

object BusinessProcessor {
  val BufSize = 1024 * 1024
//  val props = Props[BusinessProcessor]

  case object SubscribePublisher
  case class PongMessage(text: String)
}
