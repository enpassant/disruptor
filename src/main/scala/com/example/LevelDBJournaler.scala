package com.example

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

class LevelDBJournaler extends Journaler {
  def init(serialization: Serialization) = {
    val options = new Options()
    options.createIfMissing(true)
    val db = factory.open(new File("/tmp/example.bin"), options)
    new LevelDBJournalerDB(serialization, db)
  }
}

class LevelDBJournalerDB(val serialization: Serialization, val db: DB)
  extends JournalerDB {
  def actor(context: ActorContext): ActorRef = ???

  def iterator = {
    val iter = db.iterator
    iter.seekToFirst()
    val serializer = serialization.findSerializerFor("data")
    new LevelDBJournalerDBIterator(serializer, iter)
  }

  def writeData(seqNr: Long, data: AnyRef): Unit = {
    val serializer = serialization.findSerializerFor(data)
    val bb = java.nio.ByteBuffer.allocate(8)
    bb.putLong(seqNr)
    db.put(bb.array, serializer.toBinary(data))
  }

  def writeSeqData(seqNr: Long, data: Seq[AnyRef]): Unit = {
    var i = seqNr
    val batch = db.createWriteBatch
    val serializer = serialization.findSerializerFor(data)
    try {
      data foreach { d =>
        //log.debug("In JournalActor save {}", i)
        val bb = java.nio.ByteBuffer.allocate(8)
        bb.putLong(i)
        batch.put(bb.array, serializer.toBinary(d))
        //if (counter != i) log.info("Key 1 is invalid. {} vs {}", i, counter)
        i = i + 1
      }
      db.write(batch)
    } finally {
      batch.close
    }
  }
}

class LevelDBJournalerDBIterator(
  val serializer: Serializer,
  val dbIterator: DBIterator)
{
  def hasNext = dbIterator.hasNext

  def read: (Long, AnyRef) = {
    val bb = ByteBuffer.wrap(dbIterator.peekNext().getKey())
    val key = bb.getLong
    val value = serializer.fromBinary(dbIterator.peekNext().getValue())
    dbIterator.next()
    (key, value)
  }

  def close() = dbIterator.close
}
