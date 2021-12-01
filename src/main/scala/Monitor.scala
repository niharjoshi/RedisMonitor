import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

import com.redis._


object Actions {

  def monitorRedis() = {

    val r = new RedisClient("localhost", 6379)
    val keys = r.keys("p-*").toList(0)
    keys.foreach(
      key => {
        val key_string = key.toList(0)
        val value = r.get(key_string) match {
          case Some(s: String) => s
          case None => ""
        }
        r.del(key_string)
        r.set(key_string.replaceFirst("p", "d"), value)
        println(value)
      }
    )
  }

}


object PrintMyActorRefActor {
  def apply(): Behavior[String] =
    Behaviors.setup(context => new PrintMyActorRefActor(context))
}


class PrintMyActorRefActor(context: ActorContext[String]) extends AbstractBehavior[String](context) {

  override def onMessage(msg: String): Behavior[String] =
    msg match {
      case "push to kafka" =>
        val secondRef = context.spawn(Behaviors.empty[String], "second-actor")
        println(s"Second: $secondRef - Pushing logs to Kafka topic")
        this
    }
}


object Main {
  def apply(): Behavior[String] =
    Behaviors.setup(context => new Main(context))

}


class Main(context: ActorContext[String]) extends AbstractBehavior[String](context) {
  override def onMessage(msg: String): Behavior[String] =
    msg match {
      case "check for table updates" =>
        val firstRef = context.spawn(PrintMyActorRefActor(), "first-actor")
        println(s"First: $firstRef - Checking DynamoDB for updates")
        Actions.monitorRedis()
        firstRef ! "push to kafka"
        this
    }
}


object ActorHierarchyExperiments extends App {
  val monitoringSystem = ActorSystem(Main(), "monitoringSystem")
  monitoringSystem ! "check for table updates"
}
