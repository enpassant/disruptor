package com.example

import akka.actor.{Actor, ActorRef, ActorLogging, Props, ReceiveTimeout}
import akka.util.Timeout
import org.joda.time.DateTime
import org.joda.time.format.{ DateTimeFormatter, ISODateTimeFormat }
import org.json4s.jackson.Serialization
import org.json4s.{ CustomSerializer, DefaultFormats, Formats,
  JNull, JString, MappingException, ShortTypeHints }
import scala.concurrent.duration._
import scala.util.Try

class SampleBusinessProcessor(bufSize: Int)
  extends BusinessProcessor(bufSize) with ActorLogging
{
  import SampleBusinessProcessor._
  import Disruptor._
  import SampleClient._
  import scala.concurrent.ExecutionContext.Implicits.global

  var msgCount = 0

  def formats: Formats = new DefaultFormats {
    override val typeHintFieldName = "_t"
    override val typeHints = ShortTypeHints(
      List(
        classOf[PingMessage],
        classOf[JournalActor.Replayed]))
  } ++ org.json4s.ext.JodaTimeSerializers.all + MyDateTimeSerializer

  def journaler = new FileJournaler("/tmp/example.bin", formats)

  type STATE = Int

  var state = 0

  val updateState: (AnyRef, Boolean) => AnyRef = (msg: AnyRef, replayed: Boolean) => {
    log.debug("SampleBusinessProcessor - updateState: {}, {}, {}",
      state, msg, replayed)
    state = state + 1
    s"Result: $state"
  }

  val pingActor2 = context.actorOf(PingActor.props, "pingActor2")
  val pingActor3 = context.actorOf(PingActor.props, "pingActor3")

  disruptor ! Consumer(2, pingActor3.path.toString, 100)
  disruptor ! Consumer(1, pingActor2.path.toString, 100)

  def receiveCommand: Receive = {
    case msg: AnyRef =>
      //log.debug("SampleBusinessProcessor - received message: {}", msg)
      disruptor ! PersistentEvent(msg.toString, msg)
      //val sndr = sender
      //persist(msg) map {
        //case msg =>
          //sndr ! msg
          //msgCount += 1
      //}
  }
}

object SampleBusinessProcessor {
  val props = Props(new SampleBusinessProcessor(21))

  case object MyDateTimeSerializer extends CustomSerializer[DateTime](format => (
    {
      case JString(s) => Try(new DateTime(s)).getOrElse(
        throw new MappingException(s"Invalid date format: $s"))
      case JNull => null
    },
    {
      case d: DateTime => JString(ISODateTimeFormat.dateTime.print(d))
    }
  ))
}
