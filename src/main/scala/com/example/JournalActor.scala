package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

class JournalActor(journaler: Journaler) extends Actor with ActorLogging {
  import JournalActor._

  val random = new scala.util.Random
  var counter = 0L

  val serialization = SerializationExtension(context.system)
  val db = journaler.init(serialization)
  val actor = db.actor(context)

  var disruptor: ActorRef = _
  var processor: ActorRef = _

  def receive: PartialFunction[Any, Unit] = {
    case ReplayNext(processor, disruptor) =>
      actor ! ReplayNext(processor, disruptor)

    case Process(0, index, id, command) =>
      log.debug(s"In JournalActor - process command")
      sender ! Processed(index, id, command)

    case Process(seqNr, index, id, Array(others @ _*)) =>
      val (replayed, remains) = others.filter {
        _ != Terminate
      } partition {
        case Replayed(elem) => true
        case _ => false
      }
      if (replayed.size > 0) sender ! Processed(index - remains.size, id, replayed)
      log.debug(s"""In JournalActor - Replayed: ${replayed mkString ", "}""")
      log.debug(s"""In JournalActor - Remains: ${remains mkString ", "}""")
      if (remains.size > 0) process(Process(seqNr, index, id, remains))

    case Processed(index, id, command: Command) =>
      log.debug(s"In JournalActor - received command message: $index, $counter, $command")

    case Processed(index, id, data) =>
      log.debug(s"In JournalActor - received process message: $index, $counter, $data")
      counter += 1

    case msg =>
      process(msg)
  }

  def process: PartialFunction[Any, Unit] = {
    case Replay(p, d, count) =>
      log.debug("Start replay")

      processor = p
      disruptor = d
      actor ! Replay(p, d, count)

    case Process(seqNr, index, id, Terminate) =>
      sender ! Processed(index, id, Terminate)

    case Process(seqNr, index, id, Replayed(data)) =>
      log.debug(s"""In JournalActor - Replayed: $data""")
      sender ! Processed(index, id, Replayed(data))

    case Process(seqNr, index, id, data: Seq[AnyRef @unchecked]) =>
      log.debug(s"In JournalActor - received process message: $index, $data")
      val serializer = serialization.findSerializerFor(data)
//      var i = index - data.size
      db.writeSeqData(seqNr, data)
      counter += data.length
      sender ! Processed(index, id, data)

    case Process(seqNr, index, id, data) =>
      log.debug(s"In JournalActor save $seqNr")
      log.debug(s"In JournalActor - received process message: $index, $data")
      db.writeData(seqNr, data)
      counter += 1
      if (counter != seqNr) log.info(s"Key 2 is invalid. $index vs $counter")
      sender ! Processed(index, id, data)

    case msg =>
      log.debug(s"In JournalActor - received message: $msg")
  }
}

object JournalActor {
  def props(journaler: Journaler) = Props(new JournalActor(journaler))

  case object Initialize
  case object ReplayFinished extends Command
  case class Replayed(msg: AnyRef)
  case class Replay(processor: ActorRef, disruptor: ActorRef, count: Long)
  case class ReplayNext(processor: ActorRef, disruptor: ActorRef)
  case class PingMessage(text: String)
}

