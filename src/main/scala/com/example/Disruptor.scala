package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

class Disruptor(bufSize: Int) extends Actor with ActorLogging {
  import Disruptor._

  val buffer = new Array[BufferItem](bufSize)
  var indexes = Array.empty[Long]

  def receive = initialize(Array(Consumer(0, "")))

  def initialize(consumers: Array[Consumer]): Receive = {
    case c: Consumer =>
      context.become(initialize((consumers :+ c).sortWith(_.order < _.order)))

    case Initialized =>
      val orders = consumers.map(_.order).toSet
      indexes = new Array[Long](orders.size)
      log.info(indexes mkString ",")
      val cs = consumers.tail.foldLeft(Vector(List(consumers.head))) {
        (a, c) => if (a.last.head.order != c.order) {
            a :+ List(c)
          }
          else {
            a.updated(a.size - 1, c :: a.last)
          }
      }
      log.info(cs mkString ",")
      context.become(process(cs))

    case GetState if log.isDebugEnabled =>
      sender ! consumers
  }

  def process(consumers: Vector[List[Consumer]]): Receive = {
    case Event(id, data) if (indexes.head - bufSize) < indexes.last =>
      buffer((indexes.head % bufSize).toInt) = BufferItem(sender, id, data)
      indexes(0) = indexes.head + 1
      log.info(data.toString)
      step(1, consumers)

    case Event(id, data) =>
      log.info(s"The event buffer is full! The $data is dropped.")
      sender ! Busy(id)

    case Processed(id) =>
      log.info(s"Received processed message: $id")
  }

  def step(index: Int, consumers: Vector[List[Consumer]]): Unit = {
    val prevIndex = indexes(index - 1)

    consumers(index).foreach { consumer =>
      val cIndex = consumer.index
      val dif = cIndex - prevIndex
      if (dif <= 0) {
        val range = (cIndex until prevIndex)
        range.foreach { i =>
          val idx = (i % bufSize).toInt
          context.actorSelection(consumer.actorPath) ! Process(consumer.actorPath, buffer(idx).data)
        }
      }
    }
  }
}

object Disruptor {
  def props(bufSize: Int) = Props(new Disruptor(bufSize))

  case object GetState
  case object Initialized
  case class Consumer(order: Int, actorPath: String) {
    var index = 0L
  }
  case class Event(id: String, data: Any)
  case class Process(id: String, data: Any)
  case class Processed(id: String)
  case class Busy(id: String)
  case class BufferItem(sender: ActorRef, id: String, data: Any)
}

