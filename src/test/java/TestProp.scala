import util.Utils

/**
  * Created by rzx on 2016/9/18.
  */
object TestProp {
  val prop = Utils.getProp()
  def main(args: Array[String]): Unit = {
    println(prop.getProperty("separator"))
  }
}
