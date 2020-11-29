import org.scalatest.FunSuite

import scala.collection.immutable.Queue

class LastNListTest extends FunSuite {
  test("list with length 5 and initialized with 3") {
    val initial = LastNList(5)(1,2,3)
    assert (initial.list == Queue(1, 2, 3))
  }

  test("take only last N if initial list longer than capacity") {
    val initial = LastNList(3)(1, 2, 3, 4, 5)
    assert (initial.list == Queue(3, 4, 5))
  }

  test("add new element: if list short just append") {
    val initial = LastNList(3)(1, 2)
    val updated = initial.update(3)
    assert (updated.list == Queue(1, 2, 3))
    assert (updated.size == initial.size + 1)
  }

  test("add new element: if list full cut first and append") {
    val initial = LastNList(3)(1, 2, 3)
    val updated = initial.update(4)
    assert (updated.list == Queue(2, 3, 4))
    assert (updated.size == initial.size)
  }

  test("get max of full LastNList") {
    val initial = LastNList(5)(3, 2, 1, 5, 2)
    assert (initial.maxList(initial).get == 5)
  }

  test("get None when calling max of short list") {
    val initial = LastNList(5)(1, 2, 3)
    assert (initial.maxList(initial).isEmpty)
  }
}
