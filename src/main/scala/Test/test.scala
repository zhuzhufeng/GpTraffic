package Test

object test {
  def main(args: Array[String]): Unit = {
    val prices = List("a","b","c")

    val quantities = List("c","d","e")

    val tuples: List[(String, String)] = prices zip quantities
    println(tuples)
    println("***********************************")
    val res: List[String] = tuples.map(x=>x._1+x._2)
    print(res)

  }

}
