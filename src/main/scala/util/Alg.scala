package util

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits._

object Global {

}

object Alg {
  def main (args: Array[String]) {
    println(curry(1)("3"))
  }

  def curry(p1 : Int)(p2 : String) : String = {
    p1 + p2
  }

  def testFutureTask (args: Array[String]) {
    val futureTask = Future {
      println("before task")
      Thread.sleep(1000)
      println("after task")

      throw new RuntimeException("asd")
      3
    }

    futureTask.map(_ * 1000).onComplete {
      case Success(s) => println("success = " + s)
      case Failure(f) => println(f)
    }

    Thread.sleep(2000)
  }
}

object TestTwitter {
  import com.twitter.finagle._
}

object Candies {
  def io = {
    val n = readLine().toInt
    println(ans(n, (1 to n).toList.map(t => readLine().toInt)))
  }

  def ans(n : Int, scores : List[Int]) : Int = {
    val candies = scores.toArray
    candies(0) = 1
    (1 until n).foreach(i => {
      if(scores(i) > scores(i - 1))
        candies(i) = candies(i - 1) + 1
      else
        candies(i) = 1
    })

    (0 until n - 1).reverse.foreach(i => {
      if(scores(i) > scores(i + 1))
        candies(i) = Math.max(candies(i), candies(i + 1) + 1)
    })

    candies.sum
  }
}

object CoinChange {
  val a = Array.ofDim(3, 2)

}
