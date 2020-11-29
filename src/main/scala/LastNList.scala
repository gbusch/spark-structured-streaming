import scala.collection.immutable.Queue

case class LastNList[T] private (capacity: Int, size: Int, list: Queue[T]) {
  def update(newElement: T): LastNList[T] = {
    if (size < capacity) LastNList(capacity, size + 1, list.enqueue(newElement))
    else LastNList(capacity, size, list.drop(1).enqueue(newElement))
  }

  def maxList[T <% Ordered[T]](list: LastNList[T]): Option[T] = {
    if (list.size == list.capacity) Option(list.list.max) else None
  }
}

object LastNList {
  def empty[T](capacity: Int): LastNList[T] = LastNList(capacity, 0, Queue[T]())

  def apply[T](capacity: Int)(xs: T*): LastNList[T] = {
    val elems = if (xs.size <= capacity) xs else xs.takeRight(capacity)
    LastNList[T](capacity, elems.size, Queue(elems: _*))
  }
}