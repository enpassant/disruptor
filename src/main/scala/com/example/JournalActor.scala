package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

class JournalActor extends Actor with ActorLogging {
  import JournalActor._
  import Disruptor._

  val random = new scala.util.Random
  var counter = 0

  val options = new Options();
  options.createIfMissing(true);
  val db = factory.open(new File("/tmp/example.bin"), options);

  val serialization = SerializationExtension(context.system)
  val data = PingMessage("test")
  val serializer = serialization.findSerializerFor(data)
  var iterator = db.iterator()

  def receive = {
    case Disruptor.Process(index, id, _, Array(others @ _*)) =>
      val (replayed, remains) = others.partition {
        case Replayed(elem) => true
        case _ => false
      }
      if (replayed.size > 0) sender ! Disruptor.Processed(index - remains.size, id)
      log.info(s"""In JournalActor - Replayed: ${replayed mkString ", "}""")
      log.info(s"""In JournalActor - Remains: ${remains mkString ", "}""")
      process(Disruptor.Process(index, id, false, remains))

    case msg =>
      process(msg)
  }

  def process: Receive = {
    case Disruptor.Process(index, id, replaying, Replay(processor)) =>
      replay(processor, sender)
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, replaying, Replayed(data)) =>
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, _, data) =>
      counter += 1
      log.info(s"In JournalActor - received process message: $index, $data")
      val serializer = serialization.findSerializerFor(data)
      val bb = java.nio.ByteBuffer.allocate(8)
      bb.putLong(index)
      db.put(bb.array, serializer.toBinary(data))
      sender ! Disruptor.Processed(index, id)

    case msg =>
      log.info(s"In JournalActor - received message: $msg")
  }

  def replay(processor: ActorRef, disruptor: ActorRef) = {
    iterator = db.iterator()
    try {
      iterator.seekToFirst()
      while (iterator.hasNext()) {
        val bb = ByteBuffer.wrap(iterator.peekNext().getKey())
        val key = bb.getLong
        val value = serializer.fromBinary(iterator.peekNext().getValue())
        value match {
          case array: Vector[AnyRef] =>
            log.info(key+" = "+ (array mkString ", "))
            array foreach { msg =>
              disruptor.tell(Event(key.toString, Replayed(msg)), processor)
            }
            Thread.sleep(1)
          case msg: AnyRef =>
            log.info(key+" = "+value)
            disruptor.tell(Event(key.toString, Replayed(msg)), processor)
        }
        iterator.next()
      }
    } finally {
      // Make sure you close the iterator to avoid resource leaks.
      iterator.close();
      disruptor.tell(Event(ReplayFinished.toString, ReplayFinished), processor)
      log.info("ReplayFinished")
    }
  }
}

object JournalActor {
  val props = Props[JournalActor]

  case object Initialize
  case class Replayed(msg: AnyRef)
  case class Replay(processor: ActorRef)
  case class PingMessage(text: String)
}

