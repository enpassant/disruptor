package com.example

import akka.actor.{Actor, ActorContext, ActorLogging, ActorRef, Props}
import java.io._
import akka.serialization._
import java.nio.ByteBuffer

import Disruptor._

trait JournalerDB {
  val serialization: Serialization

  def writeData(seqNr: Long, data: AnyRef): Unit
  def writeSeqData(seqNr: Long, data: Seq[AnyRef]): Unit
  def actor(context: ActorContext): ActorRef
}

trait Journaler {
  def init(serialization: Serialization): JournalerDB
}
