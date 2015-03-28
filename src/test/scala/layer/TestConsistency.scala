package layer

import java.util.Properties

import org.junit.Test
import org.junit.Assert
import partition._

import scala.util.{Failure, Success, Try}

class TestConsistency {
  private val props = new Properties()
  props.setProperty("quorum", "3")
  props.setProperty("write", "2")
  props.setProperty("read", "2")
  private val dummyClient = new ConsistencyClient(null, null, null, props)
  private val content = "hello".getBytes

  @Test
  def testVersionedReconcile = {
    val vc1 = VectorClock(Map("s1"->1, "s2"->1, "s3"->1))
    val vc2 = VectorClock(Map("s1"->2, "s2"->1, "s3"->2))

    val ans1 = reconcile(vc1, vc2)
    Assert.assertEquals(1, ans1.size)
    Assert.assertEquals(vc2, ans1(0).version)

    val vc3 = VectorClock(Map("s1"->1, "s2"->2, "s3"->1, "s4"->2))
    val ans2 = reconcile(vc1, vc3)
    Assert.assertEquals(1, ans1.size)
    Assert.assertEquals(vc3, ans2(0).version)

    val ans3 = reconcile(vc2, vc3)
    Assert.assertEquals(2, ans3.size)

    val ans4 = reconcileList(List(vc1, vc2, vc3))
    Assert.assertEquals(2, ans4.size)

    val vc4 = VectorClock(Map("s5" -> 1))
    val vc5 = VectorClock(Map("s6" -> 1))
    val vc6 = VectorClock(Map("s5" -> 1, "s6" -> 2))

    val ans5 = reconcileList(List(vc1, vc2, vc3, vc4, vc5, vc6))
    Assert.assertEquals(3, ans5.size)
  }

  private def reconcile(vc1 : VectorClock, vc2 : VectorClock) =
    dummyClient.reconcile(List(Versioned(vc1, content), Versioned(vc2, content)))

  private def reconcileList(list : List[VectorClock]) =
    dummyClient.reconcile(list.map(t => Versioned(t, content)))


  @Test
  def testConsistencyClientWithNoNodeFailure : Unit = {
    val local = "127.0.0.1"
    val ports = 1 to 10 map (_ + 8550)
    val nodes = ports map (p => Node(local, p))

    val metaServer = new MetaServer()
    nodes.foreach(n => metaServer.addServer(n))

    val nextLayerBuilder : Node => Layer = {
      val layers = nodes.map(n => n -> new InMemStorageLayer).toMap
      (n : Node) => layers(n)
    }

    val key1 = "key1"
    val vc1 = VectorClock(Map("s1"->1, "s2"->1, "s3"->1))
    val ver1 = new Versioned(vc1, content)

    val consistencyClient = new ConsistencyClient(nodes(0), metaServer, nextLayerBuilder, props)
    val dummyContext = Map[String, AnyRef]()

    consistencyClient.put(key1, ver1, dummyContext)
    val ans1 = consistencyClient.get(key1, dummyContext)
    Assert.assertTrue(ans1.isInstanceOf[Success[_]])
    Assert.assertEquals(1, ans1.get.size)

    val vc2 = VectorClock(Map("s1"->2, "s2"->2, "s3"->2))
    val ver2 = new Versioned(vc2, content)
    consistencyClient.put(key1, ver2, dummyContext)
    val ans2 = consistencyClient.get(key1, dummyContext)
    Assert.assertTrue(ans2.isInstanceOf[Success[_]])
    Assert.assertEquals(1, ans2.get.size)
    Assert.assertEquals("{\"s1\" : 2, \"s2\" : 2, \"s3\" : 2, \"127.0.0.1:8551\" : 1}",
                (ans2.get.toList)(0).version.toString)

    val vc3 = VectorClock(Map("s1"->1, "s4"->2))
    val ver3 = new Versioned(vc3, content)
    consistencyClient.put(key1, ver3, dummyContext)
    val ans3 = consistencyClient.get(key1, dummyContext)
    Assert.assertTrue(ans3.isInstanceOf[Success[_]])
    Assert.assertEquals(2, ans3.get.size)


    val key2 = "key2"
    consistencyClient.put(key2, ver1, dummyContext)
    consistencyClient.put(key2, ver2, dummyContext)
    consistencyClient.put(key2, ver3, dummyContext)
    val ans4 = consistencyClient.get(key2, dummyContext)
    Assert.assertTrue(ans4.isInstanceOf[Success[_]])
    Assert.assertEquals(2, ans4.get.size)
  }

  private class InMemStorageLayer extends Layer {
    private val cache = scala.collection.mutable.Map[String, Iterable[Versioned]]()

    def get(key : String, context : Map[String, AnyRef]) : Try[Iterable[Versioned]] = {
      cache.get(key) match {
        case Some(r) => Success(r)
        case None => Failure(new RuntimeException("no such element"))
      }
    }

    def put(key : String, value : Versioned, context : Map[String, AnyRef]) : Try[Unit] = {
      cache.put(key, cache.getOrElse(key, List[Versioned]()).toList :+ value)
      Success()
    }
  }
}
