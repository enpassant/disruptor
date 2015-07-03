package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import scala.concurrent.duration._

class SampleBusinessProcessor extends BusinessProcessor with ActorLogging {
  import Disruptor._

  var msgCount = 0

  def receiveCommand: Receive = {
    case msg: AnyRef =>
      log.debug(s"In SampleBusinessProcessor - received message: $msg")
      disruptor ! PersistentEvent(msg.toString, msg)
  }

  def receiveRecover: Receive = {
    case msg =>
      msgCount += 1
  }
}

object SampleBusinessProcessor {
  val props = Props(new SampleBusinessProcessor)
}
