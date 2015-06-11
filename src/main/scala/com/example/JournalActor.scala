package com.example

import akka.actor.{Actor, ActorLogging, Props}
import org.iq80.leveldb._
import org.fusesource.leveldbjni.JniDBFactory._
import java.io._
import akka.serialization._

class JournalActor extends Actor with ActorLogging {
  import JournalActor._

  val random = new scala.util.Random
  var counter = 0

  val options = new Options();
  options.createIfMissing(true);
  val db = factory.open(new File("/tmp/example.bin"), options);

  val serialization = SerializationExtension(context.system)

  def receive = {
  	case Initialize =>
	    log.info("In JournalActor - starting ping-pong")

  	case Disruptor.Process(index, id, data) =>
          counter += 1
// 	  log.info(s"In JournalActor - received process message: $counter")
          val serializer = serialization.findSerializerFor("data a sd asd a sd asd a sd asd as d asd")
          db.put(bytes(index.toString), serializer.toBinary(data.toString))
          sender ! Disruptor.Processed(index, id)
  	case msg =>
  	  log.info(s"In JournalActor - received message: $msg")
  }
}

object JournalActor {
  val props = Props[JournalActor]
  case object Initialize
  case class PingMessage(text: String)
}

