package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

class JournalActor extends Actor with ActorLogging {
  import JournalActor._

  val random = new scala.util.Random
  var counter = 0L

  val journaler = new LevelDBJournaler
  val serialization = SerializationExtension(context.system)
  val db = journaler.init(serialization)

  var iterator: Option[JournalerDBIterator] = None
  var disruptor: ActorRef = _
  var processor: ActorRef = _

  def receive = {
    case ReplayNext(processor, disruptor) =>
      sendNext(1)

    case Disruptor.Process(0, index, id, command) =>
      log.debug(s"In JournalActor - process command")
      sender ! Disruptor.Processed(index, id, command)

    case Disruptor.Process(seqNr, index, id, Array(others @ _*)) =>
      val (replayed, remains) = others.filter {
        _ != Terminate
      } partition {
        case Replayed(elem) => true
        case _ => false
      }
      if (replayed.size > 0) sender ! Disruptor.Processed(index - remains.size, id, replayed)
      log.debug(s"""In JournalActor - Replayed: ${replayed mkString ", "}""")
      log.debug(s"""In JournalActor - Remains: ${remains mkString ", "}""")
      if (remains.size > 0) process(Disruptor.Process(seqNr, index, id, remains))

    case Disruptor.Processed(index, id, data) =>
      log.debug(s"In JournalActor - received process message: $index, $counter, $data")
      sendNext(1)

    case msg =>
      process(msg)
  }

  def process: Receive = {
    case Replay(p, d, count) =>
      processor = p
      disruptor = d
      replay(count)

    case Disruptor.Process(seqNr, index, id, Terminate) =>
      sender ! Disruptor.Processed(index, id, Terminate)

    case Disruptor.Process(seqNr, index, id, Replayed(data)) =>
      log.debug(s"""In JournalActor - Replayed: $data""")
      sender ! Disruptor.Processed(index, id, Replayed(data))

    case Disruptor.Process(seqNr, index, id, data: Seq[AnyRef @unchecked]) =>
      log.debug(s"In JournalActor - received process message: $index, $data")
      val serializer = serialization.findSerializerFor(data)
//      var i = index - data.size
      db.writeSeqData(seqNr, data)
      counter += data.length
      sender ! Disruptor.Processed(index, id, data)

    case Disruptor.Process(seqNr, index, id, data) =>
      log.debug(s"In JournalActor save $seqNr")
      log.debug(s"In JournalActor - received process message: $index, $data")
      db.writeData(seqNr, data)
      counter += 1
      if (counter != seqNr) log.info(s"Key 2 is invalid. $index vs $counter")
      sender ! Disruptor.Processed(index, id, data)

    case msg =>
      log.debug(s"In JournalActor - received message: $msg")
  }

  def sendNext(count: Long) = {
    log.debug("sendNext")

    iterator foreach { iter =>
      log.debug(s"isNext: ${iter.hasNext}")
      var i = 0
      while (iter.hasNext && i < count) {
        i += 1
        val (key, value) = iter.read
        log.debug(s"Replay: $key")
        value match {
          case array: Vector[AnyRef @unchecked] =>
            log.debug(key+" = "+ (array mkString ", "))
            array foreach { msg =>
              counter += 1
              if (counter != key) log.info(s"Key 3 is invalid. $key vs $counter")
              disruptor.tell(PersistentEvent(key.toString, Replayed(msg)), self)
            }
          case msg: AnyRef =>
            log.debug(key+" = "+value)
            counter += 1
            if (counter != key) log.info(s"Key 4 is invalid. $key vs $counter")
            disruptor.tell(PersistentEvent(key.toString, Replayed(msg)), self)
        }
      }
      if (!iter.hasNext) {
        log.info(s"Close Replay: $counter")

        // Make sure you close the iterator to avoid resource leaks.
        iter.close
        iterator = None
        disruptor.tell(ReplayFinished, self)
//        processor ! ReplayFinished
      }
    }
  }

  def replay(count: Long) = {
    log.debug("Start replay")

    val iter = db.iterator
    iterator = Some(iter)
    sendNext(count)
  }
}

object JournalActor {
  val props = Props[JournalActor]

  case object Initialize
  case object ReplayFinished extends Disruptor.Command
  case class Replayed(msg: AnyRef)
  case class Replay(processor: ActorRef, disruptor: ActorRef, count: Long)
  case class ReplayNext(processor: ActorRef, disruptor: ActorRef)
  case class PingMessage(text: String)
}

