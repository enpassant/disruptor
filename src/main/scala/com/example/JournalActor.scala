package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

class JournalActor(journaler: Journaler) extends Actor with ActorLogging {
  import JournalActor._

  val random = new scala.util.Random
  var counter = 1L

  val serialization = SerializationExtension(context.system)
  val db = journaler.init(serialization)
  val actor = db.actor(context)

  var disruptor: ActorRef = _

  def receive: PartialFunction[Any, Unit] = {
    case ReplayNext(disruptor) =>
      actor ! ReplayNext(disruptor)

    case Process(seqNr, index, id, command: Command) =>
      log.debug("In JournalActor - process command")
      sender ! Processed(index, id, None)

    case Process(seqNr, index, id, others: Seq[AnyRef @unchecked]) =>
      val (replayed, remains) = others.filter {
        _ != Terminate
      } partition {
        case Replayed(elem) => true
        case _ => false
      }
      if (replayed.size > 0) {
        counter += replayed.size
        sender ! Processed(index - remains.size, id, None)
      }
      log.debug("In JournalActor - Replayed: {}", replayed mkString ", ")
      log.debug("In JournalActor - Remains: {}", remains mkString ", ")
      if (remains.size > 0) process(Process(seqNr + replayed.size, index, id, remains))

    case Processed(index, id, Replayed(data)) =>
      actor ! ReplayNext(disruptor)

    case Processed(index, id, command: Command) =>
      log.debug("In JournalActor - received command message: {}, {}, {}",
        index, counter, command)

    case Processed(index, id, data) =>
      log.debug("In JournalActor - received process message: {}, {}, {}",
        index, counter, data)

    case msg =>
      log.debug("In JournalActor - process: {}", msg)
      process(msg)
  }

  def process: PartialFunction[Any, Unit] = {
    case Replay(d, count) =>
      log.debug("Start replay (count: {})", count)

      disruptor = d
      actor ! Replay(d, count)

    case Process(seqNr, index, id, Terminate) =>
      sender ! Processed(index, id, Terminate)

    case Process(seqNr, index, id, Replayed(data)) =>
      log.debug("In JournalActor - Replayed: {}", data)
      counter += 1
      sender ! Processed(index, id, None)

    case Process(seqNr, index, id, data: Seq[AnyRef @unchecked]) =>
      log.debug("In JournalActor - received process message: {}, {}", index, data)
      val serializer = serialization.findSerializerFor(data)
//      var i = index - data.size
      db.writeSeqData(seqNr, data)
      if (counter != seqNr) log.info("Key is invalid. {} vs {}", seqNr, counter)
      counter += data.length
      sender ! Processed(index, id, None)

    case Process(seqNr, index, id, data) =>
      log.debug("In JournalActor save {}", seqNr)
      log.debug("In JournalActor - received process message: {}, {}", index, data)
      db.writeData(seqNr, data)
      if (counter != seqNr) log.info("Key 2 is invalid. {} vs {}", seqNr, counter)
      counter += 1
      sender ! Processed(index, id, None)

    case msg =>
      log.debug("In JournalActor - received message: {}", msg)
  }
}

object JournalActor {
  def props(journaler: Journaler) = Props(new JournalActor(journaler))

  case object Initialize
  case object ReplayFinished extends Command
  case class Replayed(msg: AnyRef)
  case class Replay(disruptor: ActorRef, count: Long)
  case class ReplayNext(disruptor: ActorRef)
}

