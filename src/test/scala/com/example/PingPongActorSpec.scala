package com.example

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.testkit.{ TestActors, TestKit, ImplicitSender }
import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll

class PingPongActorSpec(_system: ActorSystem) extends TestKit(_system)
  with ImplicitSender with WordSpecLike with Matchers with BeforeAndAfterAll
{

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Ping actor" must {
    "send back a Processed on a Process" in {
      val pingActor = system.actorOf(PingActor.props)
      pingActor !
        Disruptor.Process(1, 0, "1", BusinessProcessor.PongMessage("pong"))
      expectMsg(Disruptor.Processed(0, "1", None))
    }
  }

  //"A Pong actor" must {
    //"send back a Processed on a Process" in {
      //val pongActor = system.actorOf(PongActor.props)
      //pongActor ! Disruptor.Process("1", PongActor.PongMessage("pong"))
      //expectMsg(Disruptor.Processed("1"))
    //}
  //}

}
