package partition

import scala.util.parsing.json.{JSON, JSONObject}

// immutable
case class VectorClock(val initial : Map[String, Int]) {
  def this() = this(Map[String, Int]())

  def update(server : String) : VectorClock = {
    if(initial.contains(server)) {
      VectorClock(initial.updated(server, initial(server) + 1))
    } else {
      VectorClock(initial + (server -> 1))
    }
  }

  def happenBefore(that : VectorClock) : Boolean = {
    initial.forall(p => that.initial.contains(p._1) && p._2 <= that.initial(p._1))
  }

  def conflictWith(that : VectorClock) : Boolean = {
    !(happenBefore(that) || that.happenBefore(this))
  }

  // this function is not really useful !
  def reconcile(that : VectorClock) : Either[Exception, VectorClock] = {
    if(conflictWith(that))
      Left(new RuntimeException("cannot reconcile conflicted vector clocks"))
    else {
      var map = Map[String, Int]()
      (initial.keys.toList ++ that.initial.keys.toList).distinct
        .map(k => (k -> maxCnt(k, initial, that.initial)))
        .foreach(p => map = map + p)

      Right(VectorClock(map))
    }
  }

  private def maxCnt(key : String, fir : Map[String, Int], sec : Map[String, Int]) = {
    if(fir.contains(key) && !sec.contains(key)) fir(key)
    else if(!fir.contains(key) && sec.contains(key)) sec(key)
    else Math.max(fir(key), sec(key))
  }

  /**
   * @return a JSON string representing the vector clock
   */
  override def toString = JSONObject(initial).toString()

  override def hashCode = initial.hashCode()
  override def equals(that : Any) =
    that.isInstanceOf[VectorClock] && that.asInstanceOf[VectorClock].initial.equals(initial)
}

object VectorClock {
  def createFromJson(json : String) = VectorClock(toMap(json))
  private def toMap(json : String) : Map[String, Int] = {
    JSON.parseFull(json).get.asInstanceOf[Map[String, Double]]  // it converts "1" to Double 1.0
      .map(p => (p._1, p._2.toInt))
  }
}