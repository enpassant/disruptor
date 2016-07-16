package com.example

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, PoisonPill, Props}
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._
import JournalActor._

class FileJournaler(val fileName: String) extends Journaler {
  def init(serialization: Serialization) = {
    new FileJournalerDB(serialization, fileName)
  }
}

class FileJournalerDB(val serialization: Serialization, val fileName: String)
  extends JournalerDB {

  val outputStream = new BufferedOutputStream(new FileOutputStream(fileName, true))

  def actor(context: ActorContext) = {
    val inputStream = new BufferedInputStream(new FileInputStream(fileName))
    val serializer = serialization.findSerializerFor("data")
    context.actorOf(FileJournaler.props(serializer, inputStream))
  }

  def iterator = {
    val inputStream = new BufferedInputStream(new FileInputStream(fileName))
    val serializer = serialization.findSerializerFor("data")
    new FileJournalerDBIterator(serializer, inputStream)
  }

  def writeData(seqNr: Long, data: AnyRef): Unit = {
    val serializer = serialization.findSerializerFor(data)
    val binData = serializer.toBinary(data)
    val bb = java.nio.ByteBuffer.allocate(4)
    bb.putInt(binData.length)
    outputStream.write(bb.array)
    outputStream.write(binData)
    outputStream.flush
  }

  def writeSeqData(seqNr: Long, data: Seq[AnyRef]): Unit = {
    var i = seqNr
    val serializer = serialization.findSerializerFor(data)
    try {
      data foreach { d =>
        //log.debug("In JournalActor save {}", i)
        val binData = serializer.toBinary(d)
        val bb = java.nio.ByteBuffer.allocate(4)
        bb.putInt(binData.length)
        outputStream.write(bb.array)
        outputStream.write(binData)
        //if (counter != i) log.info("Key 1 is invalid. {} vs {}", i, counter)
        i = i + 1
      }
    } finally {
      outputStream.flush
    }
  }
}

class FileJournalerActor(val serializer: Serializer, val inputStream: InputStream)
  extends Actor with ActorLogging {

  var iter = new FileJournalerDBIterator(serializer, inputStream)
  var counter = 0

  def sendNext(count: Long, disruptor: ActorRef) = {
    log.debug("sendNext")

    log.debug("isNext: {}", iter.hasNext)
    var i = 0
    while (iter.hasNext && i < count) {
      i += 1
      val (key, value) = iter.read
      log.debug("Replay: {}", key)
      value match {
        case array: Vector[AnyRef @unchecked] =>
          log.debug("{} = {}", key, (array mkString ", "))
          array foreach { msg =>
            counter += 1
            if (counter != key) log.info("Key 3 is invalid. {} vs {}", key, counter)
            disruptor.tell(PersistentEvent(key.toString, Replayed(msg)), context.parent)
          }
        case msg: AnyRef =>
          log.debug("{} = {}", key, value)
          counter += 1
          if (counter != key) log.info("Key 4 is invalid. {} vs {}", key, counter)
          disruptor.tell(PersistentEvent(key.toString, Replayed(msg)), context.parent)
      }
    }
    if (!iter.hasNext) {
      log.info("Close Replay: {}", counter)

      disruptor.tell(ReplayFinished, context.parent)
      context.become(finished)
    }
  }

  def receive = {
    case Replay(disruptor, count) =>
      sendNext(count, disruptor)

    case ReplayNext(disruptor) =>
      sendNext(1, disruptor)
  }

  def finished: Receive = {
    case PoisonPill =>
      iter.close
  }
}

class FileJournalerDBIterator(val serializer: Serializer, val inputStream: InputStream) {
  var index = 0L

  def hasNext = inputStream.available > 0

  def read: (Long, AnyRef) = {
    val bufLength = new Array[Byte](4)
    inputStream.read(bufLength)
    val bb = ByteBuffer.wrap(bufLength)
    val length = bb.getInt
    val buf = new Array[Byte](length)
    inputStream.read(buf)
    val value = serializer.fromBinary(buf)
    index += 1
    (index, value)
  }

  def close() = inputStream.close
}

object FileJournaler {
  def props(serializer: Serializer, inputStream: InputStream) =
    Props(new FileJournalerActor(serializer, inputStream))
}
