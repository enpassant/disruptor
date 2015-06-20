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
  var counter = 0

  val options = new Options();
  options.createIfMissing(true);
  val db = factory.open(new File("/tmp/example.bin"), options);

  val serialization = SerializationExtension(context.system)
  val data = PingMessage("test")
  val serializer = serialization.findSerializerFor(data)
  var iterator: Option[DBIterator] = None

  def receive = {
    case ReplayNext(processor, disruptor) =>
      sendNext(processor, disruptor, 1)

    case Disruptor.Process(index, id, _, Array(others @ _*)) =>
      val (replayed, remains) = others.filter {
        _ != Terminate
      } partition {
        case Replayed(elem) => true
        case _ => false
      }
      if (replayed.size > 0) sender ! Disruptor.Processed(index - remains.size, id)
      log.debug(s"""In JournalActor - Replayed: ${replayed mkString ", "}""")
      log.debug(s"""In JournalActor - Remains: ${remains mkString ", "}""")
      if (remains.size > 0) process(Disruptor.Process(index, id, false, remains))

    case msg =>
      process(msg)
  }

  def process: Receive = {
    case Replay(processor, disruptor, count) =>
      replay(processor, disruptor, count)

    case Disruptor.Process(index, id, replaying, Terminate) =>
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, replaying, Replayed(data)) =>
      log.debug(s"""In JournalActor - Replayed: $data""")
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, _, data: Seq[AnyRef]) =>
      counter += 1
      log.debug(s"In JournalActor - received process message: $index, $data")
      val serializer = serialization.findSerializerFor(data)
      var i = index - data.size
      val batch = db.createWriteBatch
      try {
        data foreach { d =>
          i = i + 1
          val bb = java.nio.ByteBuffer.allocate(8)
          bb.putLong(i)
          batch.put(bb.array, serializer.toBinary(d))
        }
        db.write(batch)
      } finally {
        batch.close
      }
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, _, data) =>
      counter += 1
      log.debug(s"In JournalActor - received process message: $index, $data")
      val serializer = serialization.findSerializerFor(data)
      val bb = java.nio.ByteBuffer.allocate(8)
      bb.putLong(index)
      db.put(bb.array, serializer.toBinary(data))
      sender ! Disruptor.Processed(index, id)

    case msg =>
      log.debug(s"In JournalActor - received message: $msg")
  }

  def sendNext(processor: ActorRef, disruptor: ActorRef, count: Long) = {
    log.debug("sendNext")

    iterator foreach { iter =>
      log.debug(s"isNext: ${iter.hasNext}")
      var i = 0
      while (iter.hasNext && i < count) {
        i += 1
        counter += 1
        val bb = ByteBuffer.wrap(iter.peekNext().getKey())
        val key = bb.getLong
        val value = serializer.fromBinary(iter.peekNext().getValue())
        value match {
          case array: Vector[AnyRef] =>
            log.debug(key+" = "+ (array mkString ", "))
            array foreach { msg =>
              disruptor.tell(PersistentEvent(key.toString, Replayed(msg)), processor)
            }
          case msg: AnyRef =>
            log.debug(key+" = "+value)
            disruptor.tell(PersistentEvent(key.toString, Replayed(msg)), processor)
        }
        iter.next()
      }
      if (!iter.hasNext) {
        log.info(s"Close Replay: $counter")

        // Make sure you close the iterator to avoid resource leaks.
        iter.close()
        iterator = None
        processor ! ReplayFinished
      }
    }
  }

  def replay(processor: ActorRef, disruptor: ActorRef, count: Long) = {
    log.debug("Start replay")

    val iter = db.iterator()
    iter.seekToFirst()
    log.debug(s"isNext: ${iter.hasNext}")
    iterator = Some(iter)
    sendNext(processor, disruptor, count)
  }
}

object JournalActor {
  val props = Props[JournalActor]

  case object Initialize
  case object ReplayFinished
  case class Replayed(msg: AnyRef)
  case class Replay(processor: ActorRef, disruptor: ActorRef, count: Long)
  case class ReplayNext(processor: ActorRef, disruptor: ActorRef)
  case class PingMessage(text: String)
}

