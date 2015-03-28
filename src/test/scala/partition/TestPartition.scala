package partition

import org.junit.Test
import org.junit.Assert._

class TestPartition {

  @Test
  def testConsistentHash = {
    val nodes = 1 to 50 map (i => Node("127.0.0.1", 9000 + i))

    val r = new MetaServer()
    nodes.foreach(n => r.addServer(n))

    val numbers = (0 to 25).toList
    val lowercaseLetters = numbers.map(n => ('a' + n).toChar)
    val uppercaseLetters = numbers.map(n => ('A' + n).toChar)

    val targets = (numbers ++ lowercaseLetters ++ uppercaseLetters).map(_.toString).map(n => r.getServer(n).toString)
    val nodesName = nodes.map(_.toString())

    // 78 keys must be assigned to at least (50 - 15) = 35 nodes to guarantee even distribution
    assertTrue(nodesName.filterNot(n => targets.contains(n)).size <= 15)
  }

  @Test
  def testVectorClock = {
    val map = Map("s1" -> 1, "s2" -> 2, "s3" -> 1)
    val json = "{\"s1\" : 1, \"s2\" : 2, \"s3\" : 1}"

    val vc1 = VectorClock(map)
    assertEquals(vc1.toString, json)

    val vc2 = VectorClock.createFromJson(json)
    assertEquals(vc2.toString, json)

    assertEquals(vc1, vc2)
  }
}
