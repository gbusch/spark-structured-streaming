import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.streaming.{LongOffset, MemoryStream}
import org.apache.spark.sql.streaming.GroupStateTimeout
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import Max5.{EventState, SimpleEvent, StateUpdate, updateState}

import scala.Double.NaN

class MoreComplicatedStreamingTest extends FunSuite with SparkSessionWrapper with DataFrameComparer {

  import spark.implicits._
  implicit val sqlCtx = spark.sqlContext

  test("Integration test") {
    // input data as memory stream
    val events = MemoryStream[SimpleEvent]
    val sessions = events.toDS()

    val processed = sessions
      .groupByKey(_.key)
      .mapGroupsWithState[EventState, StateUpdate](GroupStateTimeout.NoTimeout())(updateState)

    val streamingQuery = processed
      .writeStream
      .format("memory")
      .queryName("queryName")
      .outputMode("update")
      .start

    // insert test data
    val offset1 = events.addData(List(
      SimpleEvent("a", 1.0),
      SimpleEvent("a", 2.0),
      SimpleEvent("a", 1.0),
      SimpleEvent("a", 5.0),
      SimpleEvent("a", 1.0),
      SimpleEvent("b", 1.0)
    ))
    streamingQuery.processAllAvailable()
    events.commit(offset1.asInstanceOf[LongOffset])

    // insert test data
    val offset2 = events.addData(List(
      SimpleEvent("a", 9.0)
    ))
    streamingQuery.processAllAvailable()
    events.commit(offset2.asInstanceOf[LongOffset])

    val offset3 = events.addData(List(
      SimpleEvent("a", 1.0),
      SimpleEvent("a", 1.0),
      SimpleEvent("a", 1.0),
      SimpleEvent("a", 1.0)
    ))
    streamingQuery.processAllAvailable()
    events.commit(offset3.asInstanceOf[LongOffset])

    val offset4 = events.addData(List(
      SimpleEvent("a", 8.0)
    ))
    streamingQuery.processAllAvailable()
    events.commit(offset4.asInstanceOf[LongOffset])

    val actualDF = spark.table("queryName")

    // create expected output
    val expectedSchema = List(
      StructField("key", StringType, true),
      StructField("max5", DoubleType, false)
    )

    val expectedData = Seq(
      Row("a", 5.0),
      Row("a", 9.0),
      Row("a", 9.0),
      Row("a", 8.0),
      Row("b", NaN)
    )

    val expectedDF = spark createDataFrame(
      spark.sparkContext.parallelize(expectedData),
      StructType(expectedSchema)
    )

    // compare actual and expected output
    assertSmallDataFrameEquality(actualDF.sort("key"), expectedDF)

  }
}
