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
  var counter = 0L
  var publishers = List.empty[ActorRef]
  var replaying = true

  type STATE

  var state: STATE

  def updateState: (AnyRef, Boolean) => AnyRef

  def journaler: Journaler

  val disruptor = context.actorOf(Disruptor.props(bufSize), "disruptor")
  val journalActor = context.actorOf(JournalActor.props(journaler), "journalActor")

  disruptor ! Consumer(1, journalActor.path.toString, 100)
  disruptor ! Consumer(100, self.path.toString, 100)

  override def preStart() {
    disruptor ! Initialized
  }

  def persist[T](msg: AnyRef): Future[Any] = {
    disruptor ? PersistentEvent(msg.toString, msg)
  }

  def receiveCommand: Receive

  def receiveRecover: PartialFunction[AnyRef, AnyRef] = {
    case JournalActor.Replayed(msg: AnyRef) =>
      updateState(msg, true)

    case msg: AnyRef =>
      updateState(msg, false)
  }

//  def persist(event: AnyRef)(handler: AnyRef => Unit): Unit = {
//    disruptor ! PersistentEvent(counter.toString, event)
//  }

  def receive = replay orElse process

  def replay: Receive = {
    case Initialized =>
      log.info("BusinessProcessor - Start replaying")
      journalActor ! Replay(disruptor, bufSize - 10)

    case SubscribePublisher =>
      publishers = publishers ++ List(sender)

    case processValue @ Disruptor.Process(0, index, id, command) =>
      log.info("BusinessProcessor - Process: {}", processValue)
      command match {
        case ReplayFinished =>
          log.info("BusinessProcessor - Start publishing")
          publishers foreach { _ ! disruptor }
          sender ! Disruptor.Processed(index, id, command)
          context.become(process orElse receiveCommand)
      }
  }

  def process: Receive = {
    case Disruptor.Process(seqNr, index, id, data) =>
      val result: AnyRef = data match {
        case seq: Seq[AnyRef @unchecked] => (seq map receiveRecover).toSeq
        case _: AnyRef => receiveRecover(data)
      }
      // FIXME: Exchange Replayed cause to stop replaying
      //sender ! Disruptor.Processed(index, id, result)
      sender ! Disruptor.Processed(index, id, None)

    case Disruptor.Processed(index, "Terminate", Terminate) =>
      log.info("BusinessProcessor - TERMINATED. Processed: {}, {}", index, counter)
      context.system.shutdown

    case Disruptor.Processed(index, id, data) =>
      counter += 1
      log.debug("BusinessProcessor - received process message: {}, {}, {}",
	index, counter, data)
  }
}

object BusinessProcessor {
  case object SubscribePublisher
  case class PongMessage(text: String)
}
