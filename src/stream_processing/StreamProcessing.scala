package sparkstreaming

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import java.util.Arrays
import java.util.Properties
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


/** Stream processing node saving Kafka messages to Cassandra.
 *
 * Messages are decoded and put into the hot_tub.current table. 
 * Table uses compound key to enable unique entries for each hour.
 */
object StreamProcessing {

    /** Main function of the StreamProcessing class.
     *
     * Serves as the entry point for the processing.
     *
     * @param args Required args string for the main function.
     */
    def main(args: Array[String]) {
        val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        val session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS hot_tub WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
        session.execute("CREATE TABLE IF NOT EXISTS hot_tub.current (city_and_loc text, time int, temperature float, PRIMARY KEY (city_and_loc, time));")
        
        
        // Init Kafka topic if not existent.
        val config = new Properties()
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
        val localKafkaAdmin = AdminClient.create(config)
        val topic = new NewTopic("currentTemp", 1, 1)
        val topics = Arrays.asList(topic)

        val topicStatus = localKafkaAdmin.createTopics(topics).values()


        val conf = new SparkConf().setMaster("local[2]").setAppName("CurrentTemperatureProcessing")
        val ssc = new StreamingContext(conf, Seconds(1))
        ssc.checkpoint("./checkpoints/")
        val kafkaConf =  Map[String, String]("metadata.broker.list" -> "localhost:9092")
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, Set("currentTemp"))

        val pairs = messages.map(x => (x._1, x._2.toDouble))

        /** Returns a tuple of (city, time, newTemp).
         *
         * Stream mapping. Splits the key into the city name, the record time and combines the values to a single return
         * tuple. Incoming keys are of form (<Cityname>:<longitude>:<latitude>:<Time>, <Temperature>)
         *
         * @return A tuple of (city, time, value).
         */
        def mappingFuncStream(key: String, value: Double): (String, Integer, Double) = {
            val keySplit = key.split(":")
            val city_and_loc = keySplit(0) + ":" + keySplit(1) + ":" + keySplit(2) 
            val time = keySplit(3).toInt
            (city_and_loc, time, value)
        }

        val stateDstream = pairs.map(x => mappingFuncStream(x._1, x._2))

        // store the result in Cassandra
        stateDstream.saveToCassandra("hot_tub", "current", SomeColumns("city_and_loc", "time", "temperature"))

        ssc.start()
        ssc.awaitTermination()
    }
}