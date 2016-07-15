package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import akka.util.Timeout
import scala.concurrent.duration._

class SampleBusinessProcessor(bufSize: Int)
  extends BusinessProcessor(bufSize) with ActorLogging
{
  import Disruptor._
  import scala.concurrent.ExecutionContext.Implicits.global

  var msgCount = 0

  def journaler = new FileJournaler("/tmp/example.bin")

  def receiveCommand: Receive = {
    case msg: AnyRef =>
      //log.debug(s"In SampleBusinessProcessor - received message: $msg")
      disruptor ! PersistentEvent(msg.toString, msg)
      //val sndr = sender
      //persist(msg) map {
        //case msg =>
          //sndr ! msg
          //msgCount += 1
      //}
  }

  def receiveRecover: Receive = {
    case JournalActor.Replayed(msg: AnyRef) =>
      //log.info(s"AuditoriumBusinessProcessor - receiveRecover replayed: $msg")

    case msg =>
      msgCount += 1
  }
}

object SampleBusinessProcessor {
  val props = Props(new SampleBusinessProcessor(100))
}
