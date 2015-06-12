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

  def receive = {
    case Replay(processor, disruptor) =>
      log.info("Replay")
      replay(processor, disruptor)

    case Initialize =>
        log.info("In JournalActor - starting ping-pong")

    case Disruptor.Process(index, id, _, Terminate) =>
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, true, data) =>
      counter += 1
      sender ! Disruptor.Processed(index, id)

    case Disruptor.Process(index, id, false, data) =>
      counter += 1
// 	  log.info(s"In JournalActor - received process message: $counter")
      val serializer = serialization.findSerializerFor(data)
      val bb = java.nio.ByteBuffer.allocate(8)
      bb.putLong(index)
      db.put(bb.array, serializer.toBinary(data))
      sender ! Disruptor.Processed(index, id)

    case msg =>
      log.info(s"In JournalActor - received message: $msg")
  }

  def replay(processor: ActorRef, disruptor: ActorRef) = {
    val data = PingMessage("test")
    val serializer = serialization.findSerializerFor(data)

    val iterator = db.iterator();
    try {
      iterator.seekToFirst()
      while (iterator.hasNext()) {
        val bb = ByteBuffer.wrap(iterator.peekNext().getKey())
        val key = bb.getLong
        val value = serializer.fromBinary(iterator.peekNext().getValue())
        value match {
          case array: Array[AnyRef] =>
            log.debug(key+" = "+ (array mkString ", "))
            array foreach { msg =>
              disruptor.tell(Event(key.toString, msg), processor)
            }
            Thread.sleep(1)
          case msg: AnyRef =>
            log.debug(key+" = "+value)
            disruptor.tell(Event(key.toString, msg), processor)
        }
        iterator.next()
      }
    } finally {
      // Make sure you close the iterator to avoid resource leaks.
      iterator.close();
      disruptor.tell(ReplayFinished, processor)
      log.info("ReplayFinished")
    }
  }
}

object JournalActor {
  val props = Props[JournalActor]

  case object Initialize
  case class Replay(processor: ActorRef, disruptor: ActorRef)
  case class PingMessage(text: String)
}

