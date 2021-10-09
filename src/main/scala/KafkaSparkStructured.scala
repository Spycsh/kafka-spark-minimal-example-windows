import org.apache.spark.sql.streaming.{GroupStateTimeout, Trigger, GroupState}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

// reimplement the KafkaSpark.scala with structured streaming
object KafkaSparkStructured{

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("kafkaSparkApp").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val inputDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe","avg")
      .load()
      .selectExpr("CAST(value AS STRING)")

    import spark.implicits._
    val query1 = inputDF.as[String].map(line => (line.split(",")(0),line.split(",")(1).toDouble))
      .groupByKey(tuple => tuple._1)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout)(mappingFunc)
      .writeStream
      .format("console")
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    query1.awaitTermination()
  }

  def mappingFunc(key: String, values: Iterator[(String, Double)], state: GroupState[(Double, Long)]) : (String, Double) = {
    // var (sum, counter) = state.getOption.getOrElse((0.0,0L))
    // for each group state grouped by key, start with (0.0,0L)
    var (sum, counter) = (0.0,0L)
    while(values.hasNext){
      sum = sum + values.next()._2
      counter = counter + 1L
    }
    sum = sum + state.getOption.getOrElse((0.0, 0L))._1
    counter = counter + state.getOption.getOrElse((0.0, 0L))._2
    val average : Double = sum / counter
    state.update((sum, counter))
    (key, average)
  }
}