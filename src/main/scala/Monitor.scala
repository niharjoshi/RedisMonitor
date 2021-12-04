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
import com.typesafe.config.ConfigFactory


object Actions {

  // Function to process logs from Redis
  def monitorRedis(): ListBuffer[String] = {

    // Getting Redis host
    val redis_host = ConfigFactory.load().getString("redis.host")

    // Getting Redis port
    val redis_port = ConfigFactory.load().getInt("redis.port")

    // Connecting to Redis server
    val r = new RedisClient(redis_host, redis_port)

    // Getting all keys to be processed
    val keys = r.keys("p-*").toList(0)
    val values = ListBuffer[String]()

    // Processing keys, re-saving them with new keys and filtering out WARN and ERROR messages
    keys.foreach(

      key => {

        val key_string = key.toList(0)

        val value = r.get(key_string) match {
          case Some(s: String) => s
          case None => ""
        }

        // Deleting processed key
        r.del(key_string)

        // Re-saving processed key in new format
        r.set(key_string.replaceFirst("p", "d"), value)

        // Filtering out WARN and ERROR messages
        if(value contains "WARN") values.addOne(value)
        if(value contains "ERROR") values.addOne(value)
      }
    )

    values
  }


  def pushToKafka(logs: Array[String]): Unit = {

    // Getting Kafka broker to connect to
    val brokerUrl = ConfigFactory.load().getString("kafka.broker")

    // Getting Kafka key serializer
    val key_serializer = ConfigFactory.load().getString("kafka.key_serializer")

    // Getting Kafka value serializer
    val value_serializer = ConfigFactory.load().getString("kafka.value_serializer")

    // Setting Kafka properties
    val props = new Properties()
    props.put("bootstrap.servers", brokerUrl)
    props.put("key.serializer", key_serializer)
    props.put("value.serializer", value_serializer)

    // Initializing Kafka producer
    val producer = new KafkaProducer[String, String](props)

    // Getting Kafka topic name to consume logs from
    val topic = ConfigFactory.load().getString("kafka.topic")

    // Putting logs into Kafka topic with key as UUID and catching any exceptions
    try {
      logs.foreach(
        log => {
          val record = new ProducerRecord[String, String](topic, UUID.randomUUID.toString, log)
          producer.send(record).get()
          producer.flush()
        }
      )
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {

      // Closing producer
      producer.close()

    }

  }

}


// Object to define second actor behaviour
object PrintMyActorRefActor {

  def apply(): Behavior[String] = {

    // Transforming actor context to next actor
    Behaviors.setup(context => new PrintMyActorRefActor(context))

  }
}


class PrintMyActorRefActor(context: ActorContext[String]) extends AbstractBehavior[String](context) {

  // Function to define second actor behavior on receiving message
  override def onMessage(msg: String): Behavior[String] =

    msg match {

      // In case of no matching logs
      case "" =>
        println("No matching logs found")
        this

      // In case of matching logs
      case s: String =>

        // Spawning empty second actor context
        val secondRef = context.spawn(Behaviors.empty[String], "second-actor")

        println(s"Second: $secondRef - Pushing logs to Kafka topic")

        val logs = s.split("\r\n")

        // Pushing to Kafka
        Actions.pushToKafka(logs)

        // Closing the actor
        Behaviors.stopped
    }
}


// Object to define first actor behaviour
object Main {

  def apply(): Behavior[String] =

  // Transforming actor context to next actor
    Behaviors.setup(context => new Main(context))

}


class Main(context: ActorContext[String]) extends AbstractBehavior[String](context) {

  // Function to define first actor behavior on receiving message
  override def onMessage(msg: String): Behavior[String] =

    msg match {

      // In case of message to check for Redis updates
      case "check for table updates" =>

        // Spawning new first actor context
        val firstRef = context.spawn(PrintMyActorRefActor(), "first-actor")

        println(s"First: $firstRef - checking Redis for updates")

        // Getting data from Redis
        val values = Actions.monitorRedis()
        firstRef ! values.mkString("\r\n")

        // Closing the actor
        Behaviors.stopped
    }
}


// Driver function for Akka system
object ActorHierarchyExperiments extends App {

  val monitoringSystem = ActorSystem(Main(), "monitoringSystem")

  // Starting actor system
  monitoringSystem ! "check for table updates"
}
