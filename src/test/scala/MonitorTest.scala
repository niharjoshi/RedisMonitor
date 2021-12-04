import org.scalatest.flatspec.AnyFlatSpec
import com.typesafe.config.ConfigFactory

class MonitorTest extends AnyFlatSpec {

  // Checking for redis host in config
  "config" should "contain redis host" in {

    // Getting host
    val host = ConfigFactory.load().getString("redis.host")

    assert(host == "log-file-generator-data.bagybp.ng.0001.use2.cache.amazonaws.com")

  }

  // Checking for redis port in config
  "config" should "contain redis port" in {

    // Getting port
    val port = ConfigFactory.load().getInt("redis.port")

    assert(port == 6379)

  }

  // Checking for kafka broker in config
  "config" should "contain kafka broker" in {

    // Getting broker
    val broker = ConfigFactory.load().getString("kafka.broker")

    assert(broker == "b-3.logfilegeneratorkafkac.c9jlb9.c7.kafka.us-east-2.amazonaws.com:9092,b-1.logfilegeneratorkafkac.c9jlb9.c7.kafka.us-east-2.amazonaws.com:9092,b-2.logfilegeneratorkafkac.c9jlb9.c7.kafka.us-east-2.amazonaws.com:9092")

  }

  // Checking for kafka key serializer in config
  "config" should "contain kafka key serializer" in {

    // Getting serializer
    val key_serializer = ConfigFactory.load().getString("kafka.key_serializer")

    assert(key_serializer == "org.apache.kafka.common.serialization.StringSerializer")

  }

  // Checking for kafka value serializer in config
  "config" should "contain kafka value serializer" in {

    // Getting serializer
    val value_serializer = ConfigFactory.load().getString("kafka.value_serializer")

    assert(value_serializer == "org.apache.kafka.common.serialization.StringSerializer")

  }

  // Checking for kafka topic in config
  "config" should "contain kafka topic" in {

    // Getting topic
    val topic = ConfigFactory.load().getString("kafka.topic")

    assert(topic == "logs")

  }

}
