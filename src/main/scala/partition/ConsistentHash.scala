package partition

import java.util.TreeMap

class ConsistentHash[H <: Comparable[H], V] (hash : String => H, replica : Int) {
  private val hashRing = new TreeMap[H, V]()

  def add(key : String, value : V): Unit = {
    (1 to replica).map(_ + key).foreach(s => hashRing.put(hash(s), value))
  }

  def remove(key : String) : Unit = {
    (1 to replica).map(_ + key).foreach(s => hashRing.remove(hash(s)))
  }

  def get(key : String) : V = {
    var h = hash(key)
    val tail = hashRing.tailMap(h)
    h = if(tail.isEmpty) hashRing.firstKey() else tail.firstKey()
    hashRing.get(h)
  }

  /** get all V starting from where the <fromKey> is assigned, to the tail of the hash ring */
  def getTail(fromKey : String) : List[V] = {
    val tail = hashRing.tailMap(hash(fromKey))

    import scala.collection.JavaConverters._
    tail.values().asScala.toList.distinct
  }

  /** get all V as list, starting from where the <fromKey> is assigned */
  def getAll(fromKey : String) : List[V] = {
    import scala.collection.JavaConverters._
    val tail = hashRing.tailMap(hash(fromKey))  // inclusive
    val head = hashRing.headMap(hash(fromKey))  // exclusive
    tail.values().asScala.toList.distinct ++ head.values().asScala.toList.distinct
  }
}
