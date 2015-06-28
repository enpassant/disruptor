package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import scala.concurrent.duration._

class SampleBusinessProcessor extends BusinessProcessor with ActorLogging {
  var msgCount = 0

  def receiveCommand: Receive = {
    case msg =>
  }

  def receiveRecover: Receive = {
    case msg =>
      msgCount += 1
  }
}

object SampleBusinessProcessor {
  val props = Props(new SampleBusinessProcessor)
}
