package com.example

import akka.actor.ActorSystem
import akka.actor.Actor
import akka.actor.Props
import akka.testkit.{ EventFilter, TestActorRef, TestActors, TestKit, TestProbe, ImplicitSender }
import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import scala.util.Success

class DisruptorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  "A Disruptor actor" must {
    "ordering Consumers" in {
      val disruptor = system.actorOf(Disruptor.props)

      def checkOrder(expectResult: Array[Disruptor.Consumer]) = {
          disruptor ! Disruptor.GetState
          val result = receiveOne(10.millis)
          result should be(expectResult)
      }

      val consumer1 = Disruptor.Consumer(1, "/user/PingActor1")
      val consumer2 = Disruptor.Consumer(2, "/user/PingActor2")
      val consumer3 = Disruptor.Consumer(3, "/user/PingActor3")
      val consumer4 = Disruptor.Consumer(4, "/user/PingActor4")
      val consumer5 = Disruptor.Consumer(5, "/user/PingActor5")
      val consumer6 = Disruptor.Consumer(6, "/user/PingActor6")

      disruptor ! consumer6
      checkOrder(Array(Disruptor.Consumer(0, ""), consumer6))

      disruptor ! consumer3
      checkOrder(Array(Disruptor.Consumer(0, ""), consumer3, consumer6))

      disruptor ! consumer4
      checkOrder(Array(Disruptor.Consumer(0, ""), consumer3, consumer4, consumer6))

      disruptor ! consumer2
      checkOrder(Array(Disruptor.Consumer(0, ""), consumer2, consumer3, consumer4, consumer6))

      disruptor ! consumer1
      checkOrder(Array(Disruptor.Consumer(0, ""), consumer1, consumer2, consumer3, consumer4, consumer6))

      disruptor ! consumer5
      checkOrder(Array(Disruptor.Consumer(0, ""), consumer1, consumer2, consumer3, consumer4, consumer5, consumer6))
    }
  }

  "A Disruptor actor" must {
    "receive an event and send to process" in {
      val disruptor = system.actorOf(Disruptor.props)

      val probe1 = TestProbe()
      val path1 = "/" + (probe1.ref.path.elements mkString "/")
      disruptor ! Disruptor.Consumer(6, path1)

      disruptor ! Disruptor.Initialized
      val data = "Test"
      disruptor ! Disruptor.Event(data)

      probe1.expectMsg(500.millis, Disruptor.Process(path1, data))
    }
  }

}

