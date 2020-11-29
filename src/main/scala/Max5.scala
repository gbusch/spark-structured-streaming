import org.apache.spark.sql.streaming.GroupState

package object Max5 {

  def updateState(key: String, events: Iterator[SimpleEvent], state: GroupState[EventState]): StateUpdate = {
    var oldState = if (state.exists) state.get else EventState(key, LastNList.empty(5))
    events.foreach(event => {
      val newList = oldState.values.update(event.value)
      oldState = EventState(key, newList)
    })
    StateUpdate(key, oldState.values.maxList(oldState.values).getOrElse(-1))
  }

  case class SimpleEvent(key: String, value: Int)

  case class EventState(key: String, values: LastNList[Int])

  case class StateUpdate(key: String, max5: Int)

}
