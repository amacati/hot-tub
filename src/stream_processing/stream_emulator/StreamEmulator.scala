package generator

import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

/** An emulator of the API stream input for testing purposes.
 *
 * Broadcasts random temperatures for two cities with Kafka under the "currentTemp" topic.
 * Sleeps after each broadcast to disable strange random skew.
 */
object StreamEmulator extends App {

    /** Returns a random value between -20 and 20 as string. */
    def getRandomVal: String = {
        ((Random.nextFloat() - 0.5) * 40).toString()
    }

    /** Returns a single string with a city:time combination.
     *
     * Times range from 0 to 23 (as string). Example: Austin:19.
     */
    def getRandomKey: String = {
        val cityCollection = Array("Austin:30.26:-97.76", "Munich:48.08:13.75", "Stockholm:59.20:18.03", "New York:40.43:-73.56")
        val time = Random.nextInt(24)
        val cityIndex = Random.nextInt(4)
        cityCollection(cityIndex) + ":" + Integer.toString(time)
    }

    // Kafka configuration.
    val topic = "currentTemp"
    val brokers = "localhost:9092"
    val rnd = new Random()

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "StreamEmulator")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")    
    val producer = new KafkaProducer[String, String](props)

    // Message producer loop.
    while (true){
        for (x <- 0 to 60) {
            val data = new ProducerRecord[String, String](topic, getRandomKey, getRandomVal)
            producer.send(data)
            print(data + "\n")
        }
        Thread.sleep(10000)
    }
    producer.close()
}