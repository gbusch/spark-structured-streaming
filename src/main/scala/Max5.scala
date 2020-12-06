import org.apache.spark.sql.streaming.GroupState

import scala.Double.NaN

package object Max5 {

  def updateState(key: String, events: Iterator[SimpleEvent], state: GroupState[EventState]): StateUpdate = {
    var oldState = if (state.exists) state.get else EventState(key, LastNList.empty(5))
    events.foreach(event => {
      val newList = oldState.values.update(event.value)
      oldState = EventState(key, newList)
    })
    state.update(oldState)
    StateUpdate(key, oldState.values.maxList(oldState.values).getOrElse(NaN))
  }

  case class SimpleEvent(key: String, value: Double)

  case class EventState(key: String, values: LastNList[Double])

  case class StateUpdate(key: String, max5: Double)

}
