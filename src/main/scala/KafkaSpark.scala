
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
object KafkaStreamAvg {

  def main(args: Array[String]) {
    val kafkaConf = Map(
      // defines where the Producer can find a one or more Brokers to determine the Leader for each topic
      "bootstrap.servers" -> "localhost:9092",
      "zookeeper.connect"                 -> "localhost:2181",
      "group.id"                          -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms"   -> "1000",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("KafkaStreamAvg")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("checkpoint")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String]( Array("avg"), kafkaConf )
    )

    val stream = messages.map{
      msg =>
        val attr = msg.value().split(",")
        (attr(0).toString, attr(1).toDouble)
    }

    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Long)]): (String, Double) = {
      val (prevSum, prevN) = state.getOption().getOrElse((0.0, 0L))
      val (sum, n) = (prevSum+value.get, prevN+1)
      state.update((sum, n))
      (key, sum/n) // average
    }
    val stateDStream = stream.mapWithState(StateSpec.function(mappingFunc _))

    stateDStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}