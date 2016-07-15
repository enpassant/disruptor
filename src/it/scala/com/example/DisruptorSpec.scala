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
    "answer Processed event when all consumers processed inorder" in {
      val disruptor = system.actorOf(props(8))

      val (probe1, path1) = addTestProbeConsumer(disruptor, 1)
      val (probe2, path2) = addTestProbeConsumer(disruptor, 1)
      val (probe3, path3) = addTestProbeConsumer(disruptor, 2)

      disruptor ! Initialized
      val data = "Test"
      disruptor ! PersistentEvent("0", data)

      probe1.expectMsg(100.millis, Process(1, 0, path1, data))
      probe2.expectMsg(100.millis, Process(1, 0, path2, data))
      disruptor ! Processed(0, path1, data)
      disruptor ! PersistentEvent("1", data)
      probe1.expectMsg(100.millis, Process(2, 1, path1, data))
      disruptor ! Processed(0, path2, data)
      probe2.expectMsg(100.millis, Process(2, 1, path2, data))
      probe3.expectMsg(100.millis, Process(1, 0, path3, data))
      disruptor ! Processed(0, path3, data)
      expectMsg(100.millis, Processed(0, "0", data))
      disruptor ! Processed(1, path1, data)
      disruptor ! Processed(1, path2, data)
      probe3.expectMsg(100.millis, Process(2, 1, path3, data))
      disruptor ! Processed(1, path3, data)
      expectMsg(100.millis, Processed(1, "1", data))
    }
  }
}

