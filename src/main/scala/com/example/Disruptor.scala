package com.example

import akka.actor.{Actor, ActorLogging, Props}

class Disruptor(bufSize: Int) extends Actor with ActorLogging {
  import Disruptor._

  val buffer = new Array[Any](bufSize)
  var indexes = Array.empty[Int]

  def receive = initialize(Array(Consumer(0, "")))

  def initialize(consumers: Array[Consumer]): Receive = {
    case c: Consumer =>
      context.become(initialize((consumers :+ c).sortWith(_.order < _.order)))

    case Initialized =>
      val orders = consumers.map(_.order).toSet
      indexes = new Array[Int](orders.size)
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
    case Event(data) if (indexes.head + 1) % bufSize != indexes.last =>
      buffer(indexes.head) = data
      indexes(0) = (indexes.head + 1) % bufSize
      log.info(data.toString)
      step(1, consumers)

    case Event(data) =>
      log.info(s"The event buffer is full! The $data is dropped.")

    case Processed(id) =>
      log.info(s"Received processed message: $id")
  }

  def step(index: Int, consumers: Vector[List[Consumer]]): Unit = {
    val prevIndex = indexes(index - 1)

    consumers(index).foreach { consumer =>
      val cIndex = consumer.index
      val dif = cIndex - prevIndex
      if (dif != 0) {
        val range = if (dif > 0) (cIndex until (prevIndex + bufSize))
          else (cIndex until prevIndex)
        range.foreach { i =>
          val idx = i % bufSize
          context.actorSelection(consumer.actorPath) ! Process(consumer.actorPath, buffer(idx))
        }
      }
    }
    if (index < indexes.size - 1) step(index + 1, consumers)
  }
}

object Disruptor {
  def props(bufSize: Int) = Props(new Disruptor(bufSize))

  case object GetState
  case object Initialized
  case class Consumer(order: Int, actorPath: String) {
    var index = 0
  }
  case class Event(data: Any)
  case class Process(id: String, data: Any)
  case class Processed(id: String)
}

