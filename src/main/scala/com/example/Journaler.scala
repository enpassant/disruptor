package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

trait JournalerDBIterator {
  val serializer: Serializer

  def hasNext: Boolean
  def read: (Long, AnyRef)
  def close(): Unit
}

trait JournalerDB {
  val serialization: Serialization

  def writeData(seqNr: Long, data: AnyRef): Unit
  def writeSeqData(seqNr: Long, data: Seq[AnyRef]): Unit
  def iterator: JournalerDBIterator
}

trait Journaler {
  def init(serialization: Serialization): JournalerDB
}
