import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import Max5._

object SparkApp extends SparkSessionWrapper {

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    val streamingInputDF = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load().as[String]

    var events =
      streamingInputDF
        .select(from_json(($"value").cast("string"), new StructType().add($"key".string).add($"value".float)).alias("json"))
        .select($"json.*").as[SimpleEvent]

    import org.apache.spark.sql.streaming.GroupStateTimeout

    val processed = events
      .groupByKey(_.key)
      .mapGroupsWithState[EventState, StateUpdate](GroupStateTimeout.NoTimeout())(updateState)

    val query = processed.writeStream
      .outputMode("update")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
