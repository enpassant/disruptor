package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

class Disruptor(bufSize: Int, maxCount: Int, testMode: Boolean) extends Actor with ActorLogging {
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
      log.debug(indexes mkString ",")
      val cs = consumers.tail.foldLeft(Vector(List(consumers.head))) {
        (a, c) => if (a.last.head.order != c.order) {
            a :+ List(c)
          }
          else {
            a.updated(a.size - 1, c :: a.last)
          }
      }
      log.debug(cs mkString ",")
      context.become(process(cs))

    case GetState if testMode =>
      sender ! consumers
  }

  def process(consumers: Vector[List[Consumer]]): Receive = events(consumers) orElse shutdown(consumers, None)

  def events(consumers: Vector[List[Consumer]]): Receive = {
    case Event(id, data) if (indexes.head - bufSize) < indexes.last =>
      buffer((indexes.head % bufSize).toInt) = BufferItem(sender, id, data)
      indexes(0) = indexes.head + 1
      log.debug(data.toString)
      step(1, consumers)

    case Event(id, Terminate) =>
      context.become(shutdown(consumers, Some(BufferItem(sender, id, Terminate))))

    case Event(id, data) =>
      log.debug(s"The event buffer is full! The $data is dropped.")
      sender ! Busy(id)
  }

  def shutdown(consumers: Vector[List[Consumer]], terminateItem: Option[BufferItem]): Receive = {
    case Processed(index, id) =>
      log.debug(s"Received processed message: Processed($index, $id), ${indexes.mkString}")
      consumers.flatten.find { c => c.processingIndex == index && c.actorPath == id } foreach {
        c =>
          c.index = c.processingIndex + 1
          c.processingIndex = -1L
          val i = consumers.indexWhere(_.contains(c))
          val lastIndex = indexes(i)
          indexes(i) = consumers(i).minBy { _.index }.index
          stepConsumer(indexes(i - 1), c)

          if (i < indexes.size - 1) {
            step(i + 1, consumers)
          } else {
            val range = (lastIndex until indexes(i))
            range.foreach { idx =>
              val bufferItem = buffer((idx % bufSize).toInt)
              log.debug(s"Full processed: ${Processed(idx, bufferItem.id)}")
              bufferItem.sender ! Processed(idx, bufferItem.id)
            }

            if (indexes(0) == indexes(i)) {
              terminateItem foreach { bufferItem =>
                bufferItem.sender ! Processed(indexes(0), bufferItem.id)
              }
            }
          }
      }
  }

  def stepConsumer(prevIndex: Long, consumer: Consumer): Unit = {
    val cIndex = consumer.index
    val dif = cIndex - prevIndex
    if (dif <= 0) {
        if (prevIndex > cIndex) {
//          val i = prevIndex - 1
//          val i = cIndex
          val i = prevIndex.min(cIndex + maxCount) - 1
          val firstIdx = (cIndex % bufSize).toInt
          val idx = (i % bufSize).toInt
          val maxIdx = if (firstIdx > idx) bufSize else idx + 1
          consumer.processingIndex = i
          val data = buffer.slice(firstIdx, maxIdx).map(_.data)
          log.debug(s"${Process(i, consumer.actorPath, data)}")
          context.actorSelection(consumer.actorPath) ! Process(i, consumer.actorPath, data)
        }
    }
  }

  def step(index: Int, consumers: Vector[List[Consumer]]): Unit = {
    val prevIndex = indexes(index - 1)
    log.debug(s"Step: $index, $prevIndex")

    consumers(index).filter(_.processingIndex == -1L).foreach { c =>
      stepConsumer(prevIndex, c)
    }
  }
}

object Disruptor {
  def props(bufSize: Int, maxCount: Int = 100, testMode: Boolean = false) = Props(new Disruptor(bufSize, maxCount, testMode))

  case object GetState
  case object Initialized
  case object Terminate
  case class Consumer(order: Int, actorPath: String) {
    var index = 0L
    var processingIndex = -1L
  }
  case class Event(id: String, data: Any)
  case class Process(index: Long, id: String, data: Any)
  case class Processed(index: Long, id: String)
  case class Busy(id: String)
  case class BufferItem(sender: ActorRef, id: String, data: Any)
}

