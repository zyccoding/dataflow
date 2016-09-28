import scala.collection.mutable.ListBuffer

/**
  * Created by rzx on 2016/9/9.
  */
object ScalaListTest {
  def main(args: Array[String]): Unit = {
    val list = ListBuffer[String]()
//    for (i <- 0 to 10) {list.::("a")}
    list.append("aaaa")
    list.append("b")
    println(list.head)
    println(list.length)

  }
}
