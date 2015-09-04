package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

class FileJournaler(val fileName: String) extends Journaler {
  def init(serialization: Serialization) = {
    new FileJournalerDB(serialization, fileName)
  }
}

class FileJournalerDB(val serialization: Serialization, val fileName: String)
  extends JournalerDB {

  val outputStream = new BufferedOutputStream(new FileOutputStream(fileName, true))

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
        //log.debug(s"In JournalActor save $i")
        val binData = serializer.toBinary(d)
        val bb = java.nio.ByteBuffer.allocate(4)
        bb.putInt(binData.length)
        outputStream.write(bb.array)
        outputStream.write(binData)
        //if (counter != i) log.info(s"Key 1 is invalid. $i vs $counter")
        i = i + 1
      }
    } finally {
      outputStream.flush
    }
  }
}

class FileJournalerDBIterator(val serializer: Serializer, val inputStream: InputStream)
  extends JournalerDBIterator {
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
