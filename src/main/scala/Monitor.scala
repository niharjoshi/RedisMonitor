import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.AbstractBehavior
import akka.actor.typed.scaladsl.ActorContext
import akka.actor.typed.scaladsl.Behaviors

import com.redis._
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ListBuffer

import java.util.UUID


object Actions {

  def monitorRedis(): ListBuffer[String] = {

    val r = new RedisClient("log-file-generator-data.bagybp.ng.0001.use2.cache.amazonaws.com", 6379)

    val keys = r.keys("p-*").toList(0)
    val values = ListBuffer[String]()

    keys.foreach(

      key => {

        val key_string = key.toList(0)

        val value = r.get(key_string) match {
          case Some(s: String) => s
          case None => ""
        }

        r.del(key_string)
        r.set(key_string.replaceFirst("p", "d"), value)

        if(value contains "WARN") values.addOne(value)
        if(value contains "ERROR") values.addOne(value)
      }
    )

    values
  }


  def pushToKafka(logs: Array[String]): Unit = {

    val props = new Properties()
    props.put("bootstrap.servers", "b-3.logfilegeneratorkafkac.c9jlb9.c7.kafka.us-east-2.amazonaws.com:9092,b-1.logfilegeneratorkafkac.c9jlb9.c7.kafka.us-east-2.amazonaws.com:9092,b-2.logfilegeneratorkafkac.c9jlb9.c7.kafka.us-east-2.amazonaws.com:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("batch.size", "5")
    props.put("linger.ms", "100")

    val producer = new KafkaProducer[String, String](props)

    val topic = "test-topic"

    println("Starting Kafka push")

    try {
      logs.foreach(
        log => {
          val record = new ProducerRecord[String, String](topic, "", log.mkString)
          val res = producer.send(record).get()
          println(res)
          producer.flush()
        }
      )
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      producer.close()
    }

  }

}


object PrintMyActorRefActor {
  def apply(): Behavior[String] =
    Behaviors.setup(context => new PrintMyActorRefActor(context))
}


class PrintMyActorRefActor(context: ActorContext[String]) extends AbstractBehavior[String](context) {

  override def onMessage(msg: String): Behavior[String] =
    msg match {
      case "" =>
        println("No matching logs found")
        this
      case s: String =>
        val secondRef = context.spawn(Behaviors.empty[String], "second-actor")
        println(s"Second: $secondRef - Pushing logs to Kafka topic")
        val logs = s.split("\r\n")
        Actions.pushToKafka(logs)
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
        println(s"First: $firstRef - Checking Redis for updates")
        val values = Actions.monitorRedis()
        firstRef ! values.mkString("\r\n")
        this
    }
}


object ActorHierarchyExperiments extends App {
  val monitoringSystem = ActorSystem(Main(), "monitoringSystem")
  monitoringSystem ! "check for table updates"
}
