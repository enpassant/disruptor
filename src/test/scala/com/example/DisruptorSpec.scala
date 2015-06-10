package com.example

import akka.actor.ActorSystem
import akka.actor.{Actor, ActorRef}
import akka.actor.Props
import akka.testkit.{ EventFilter, TestActorRef, TestActors, TestKit, TestProbe, ImplicitSender }
import org.scalatest.WordSpecLike
import org.scalatest.Matchers
import org.scalatest.BeforeAndAfterAll
import scala.concurrent.duration._
import scala.concurrent.Await
import akka.pattern.ask
import scala.util.Success

import Disruptor._

class DisruptorSpec(_system: ActorSystem) extends TestKit(_system) with ImplicitSender
  with WordSpecLike with Matchers with BeforeAndAfterAll {

  def this() = this(ActorSystem("MySpec"))

  override def afterAll {
    TestKit.shutdownActorSystem(system)
  }

  def addTestProbeConsumer(disruptor: ActorRef, order: Int) = {
    val probe = TestProbe()
    val path = "/" + (probe.ref.path.elements mkString "/")
    disruptor ! Consumer(order, path)
    (probe, path)
  }

  "A Disruptor actor" must {
    "ordering Consumers" in {
      val disruptor = system.actorOf(props(8, true))

      def checkOrder(expectResult: Array[Consumer]) = {
          disruptor ! GetState
          val result = receiveOne(10.millis)
          result should be(expectResult)
      }

      val consumer1 = Consumer(1, "/user/PingActor1")
      val consumer2 = Consumer(2, "/user/PingActor2")
      val consumer3 = Consumer(3, "/user/PingActor3")
      val consumer4 = Consumer(4, "/user/PingActor4")
      val consumer5 = Consumer(5, "/user/PingActor5")
      val consumer6 = Consumer(6, "/user/PingActor6")

      disruptor ! consumer6
      checkOrder(Array(Consumer(0, ""), consumer6))

      disruptor ! consumer3
      checkOrder(Array(Consumer(0, ""), consumer3, consumer6))

      disruptor ! consumer4
      checkOrder(Array(Consumer(0, ""), consumer3, consumer4, consumer6))

      disruptor ! consumer2
      checkOrder(Array(Consumer(0, ""), consumer2, consumer3, consumer4, consumer6))

      disruptor ! consumer1
      checkOrder(Array(Consumer(0, ""), consumer1, consumer2, consumer3, consumer4, consumer6))

      disruptor ! consumer5
      checkOrder(Array(Consumer(0, ""), consumer1, consumer2, consumer3, consumer4, consumer5, consumer6))
    }

    "receive an event and send to process" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 6)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("1", data)

      probe1.expectMsg(500.millis, Process(0, path1, data))
    }

    "answer Busy event if too much event received" in {
      val bufSize = 8
      val disruptor = system.actorOf(props(bufSize))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 6)

      disruptor ! Initialized
      val data = "Test"

      (1 to bufSize) foreach { idx =>
        disruptor ! Event("1", data)
        expectNoMsg(10.millis)
      }

      disruptor ! Event("2", data)
      expectMsg(100.millis, Busy("2"))
    }

    "send event to every independent consumers" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("1", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
    }

    "send event to every independent consumers and no others" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("1", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
      probe3.expectNoMsg(100.millis)
    }

    "send event to every independent consumers and no others if only one processed" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("1", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
      probe1.ref ! Processed(0, path1)
      probe3.expectNoMsg(100.millis)
    }

    "send event to every independent consumers and when it processed send to next" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("1", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
      disruptor ! Processed(0, path1)
      disruptor ! Processed(0, path2)
      probe3.expectMsg(100.millis, Process(0, path3, data))
    }

    "answer Processed event when all consumers processed" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("1", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
      disruptor ! Processed(0, path1)
      disruptor ! Processed(0, path2)
      probe3.expectMsg(100.millis, Process(0, path3, data))
      disruptor ! Processed(0, path3)
      expectMsg(100.millis, Processed(0, "1"))
    }

    "answer Processed event when all consumers processed inorder" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("0", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
      disruptor ! Processed(0, path1)
      disruptor ! Event("1", data)
      probe1.expectMsg(100.millis, Process(1, path1, data))
      disruptor ! Processed(0, path2)
      probe2.expectMsg(100.millis, Process(1, path2, data))
      probe3.expectMsg(100.millis, Process(0, path3, data))
      disruptor ! Processed(0, path3)
      expectMsg(100.millis, Processed(0, "0"))
      disruptor ! Processed(1, path1)
      disruptor ! Processed(1, path2)
      probe3.expectMsg(100.millis, Process(1, path3, data))
      disruptor ! Processed(1, path3)
      expectMsg(100.millis, Processed(1, "1"))
    }

    "answer Processed event when all consumers processed inorder 2" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! Event("0", data)

      probe1.expectMsg(100.millis, Process(0, path1, data))
      probe2.expectMsg(100.millis, Process(0, path2, data))
      disruptor ! Processed(0, path1)
      disruptor ! Event("1", data)
      probe1.expectMsg(100.millis, Process(1, path1, data))
      disruptor ! Processed(0, path2)
      probe2.expectMsg(100.millis, Process(1, path2, data))
      probe3.expectMsg(100.millis, Process(0, path3, data))
      disruptor ! Processed(1, path1)
      disruptor ! Processed(1, path2)
      disruptor ! Processed(0, path3)
      probe3.expectMsg(100.millis, Process(1, path3, data))
      expectMsg(100.millis, Processed(0, "0"))
      disruptor ! Processed(1, path3)
      expectMsg(100.millis, Processed(1, "1"))
    }

    "answer many Processed event when all consumers processed" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized

      for (i <- 0 until 10000) {
        val data = "Test"
        disruptor ! Event(i.toString, data)

        probe1.expectMsg(100.millis, Process(i, path1, data))
        probe2.expectMsg(100.millis, Process(i, path2, data))
        disruptor ! Processed(i, path1)
        disruptor ! Processed(i, path2)
        probe3.expectMsg(100.millis, Process(i, path3, data))
        disruptor ! Processed(i, path3)
        expectMsg(100.millis, Processed(i, i.toString))
      }
    }
  }

}

