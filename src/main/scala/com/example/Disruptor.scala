package com.example

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

class Disruptor(bufSize: Int, testMode: Boolean) extends Actor with ActorLogging {
  import Disruptor._

  val buffer = new Array[BufferItem](bufSize)
  var indexes = Array.empty[Long]
  var seqNr = 0

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

  def process(consumers: Vector[List[Consumer]]): Receive =
    events(consumers) orElse shutdown(consumers, None)

  def events(consumers: Vector[List[Consumer]]): Receive = {
    case data: Command =>
      log.debug(s"Received command message: $data")
      buffer((indexes.head % bufSize).toInt) = BufferItem(0, sender, data.toString, data)
      indexes(0) = indexes.head + 1
      log.debug(data.toString)
      step(1, consumers)

    case PersistentEvent(id, Terminate) =>
      context.become(shutdown(consumers, Some(BufferItem(0, sender, id, Terminate))))

    case PersistentEvent(id, data) if (indexes.head - bufSize) < indexes.last =>
      seqNr += 1
      buffer((indexes.head % bufSize).toInt) = BufferItem(seqNr, sender, id, data)
      indexes(0) = indexes.head + 1
      log.debug(data.toString)
      step(1, consumers)

    case PersistentEvent(id, data) =>
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
          val i1 = prevIndex.min(cIndex + consumer.maxCount) - 1
          val firstIdx = (cIndex % bufSize).toInt
          val idx = (i1 % bufSize).toInt
          val maxIdx = if (firstIdx > idx) bufSize else idx + 1
          val i = cIndex + maxIdx - firstIdx - 1
          val firstSeqNr = buffer(firstIdx).seqNr
          val (data, count) = firstSeqNr match {
            case 0 => (buffer(firstIdx).data, 1)
            case _ => (maxIdx - firstIdx) match {
              case 1 => (buffer(firstIdx).data, 1)
              case _ =>
                val seq = buffer.slice(firstIdx, maxIdx).takeWhile(_.seqNr > 0).map(_.data)
                (seq, seq.size)
            }
          }
          consumer.processingIndex = cIndex + count - 1
          val process = Process(firstSeqNr, consumer.processingIndex, consumer.actorPath, data)
          log.debug(s"$process")
          context.actorSelection(consumer.actorPath) ! process
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
  def props(bufSize: Int, testMode: Boolean = false) = Props(new Disruptor(bufSize, testMode))

  sealed trait Message extends AnyRef
  trait Command extends Message
  trait Event extends Message
  trait ConsumerCommand extends Command

  case object GetState
  case object Initialized
  case object Terminate

  case class Consumer(order: Int, actorPath: String, maxCount: Int = 100) {
    var index = 0L
    var processingIndex = -1L
  }
  case class PersistentEvent(id: String, data: AnyRef)
  case class Process(seqNr: Long, index: Long, id: String, data: AnyRef)
  case class Processed(index: Long, id: String)
  case class Busy(id: String)
  case class BufferItem(seqNr: Long, sender: ActorRef, id: String, data: AnyRef)
}

