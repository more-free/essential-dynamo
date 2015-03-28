package layer

import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import partition.{Versioned, MetaServer, Node}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Try, Success, Failure}
import scala.concurrent.ExecutionContext.Implicits._


/**
 * this is processed on the coordinator , upon receiving RPC calls from routing layer (the call might come from
 * any node)
 * this is the only place to deal with vector clock; quorum; hinted hand-off; etc.
 *
 * @param nextLayerBuilder it generates client for the next layer (typically the persistent storage layer).
 *                         it should be asynchronous & non-blocking (ex. Apache HttpAsyncClient)
 */
class ConsistencyClient(self : Node,
                        metaServer : MetaServer,
                        nextLayerBuilder : Node => Layer,
                        config : Properties) extends Layer {
  private val quorum = config.getProperty("quorum").toInt
  private val read = config.getProperty("read").toInt
  private val write = config.getProperty("write").toInt

  override def get(key : String, context : Map[String, AnyRef]) : Try[Iterable[Versioned]] = {
    val nodes = metaServer.getAll(key)
    if(nodes.size < read)
      return Failure(new RuntimeException("no enough nodes available !"))

    val countDownLatch = new CountDownLatch(read)
    val versions = scala.collection.mutable.Map[Node, Iterable[Versioned]]()
    val next : () => Node = nextNode(nodes)

    def send(node : Node, nextLayer : Layer) : Unit = {
      nextLayer.getAsync(key, context) onSuccess {
        case Success(r) =>
          versions += node -> r
          countDownLatch.countDown()

        case Failure(e) =>
          val n = next()
          send(n, nextLayerBuilder(n))
      }
    }

    nodes.take(quorum).map(n => (n, nextLayerBuilder(n))).foreach(p => send(p._1, p._2))
    countDownLatch.await()

    Success(reconcile(versions.values.flatten.toList))
  }

  /**
   *  weak reconcile.
   *  return a list of Versioned objects. If all Versioned objects can be completely reconciled,
   *  the size of the returned list will be one.
   */
  def reconcile(versions : List[Versioned]) : List[Versioned] = {
    if(versions.size <= 1) versions
    else if(versions.size == 2) versions(0).reconcile(versions(1))
    else {
      val mid = versions.size / 2
      merge(reconcile(versions.take(mid)), reconcile(versions.drop(mid)))
    }
  }

  private def merge(fir : List[Versioned], sec : List[Versioned]) : List[Versioned] = {
    val crossProd = for {
      i <- 0 until fir.size
      j <- 0 until sec.size
    } yield (i, j)

    val firRemoval = ArrayBuffer[Int]()
    val secRemoval = ArrayBuffer[Int]()
    crossProd.foreach(p =>
      if(fir(p._1).happenBefore(sec(p._2)))
        firRemoval += p._1
      else if(sec(p._2).happenBefore(fir(p._1)))
        secRemoval += p._2
    )

    removeByIndex(fir, firRemoval) ++ removeByIndex(sec, secRemoval)
  }

  private def removeByIndex(ori : List[Versioned], removal : ArrayBuffer[Int]) : List[Versioned] = {
    ori zip (0 until ori.size) filter (p => !removal.contains(p._2)) map (_._1)
  }


  override def put(key : String, versioned : Versioned, context : Map[String, AnyRef]) : Try[Unit] = {
    val newVersion = Versioned(versioned.version.update(self.toString()), versioned.value)

    // asynchronously send 'put' request to first N - 1 (except self) healthy nodes, and wait for W to return
    val nodes = metaServer.getAll(key)

    if(nodes.size < write)
      return Failure(new RuntimeException("no enough nodes available !"))

    val countDownLatch = new CountDownLatch(write)
    val next : () => Node = nextNode(nodes)

    def send(node : Node, nextLayer : Layer, context : Map[String, AnyRef]) : Unit = {
      nextLayer.putAsync(key, newVersion, context) onSuccess {
        case Success(_) => countDownLatch.countDown()
        case Failure(_) =>
          val n = next()
          send(n, nextLayerBuilder(n), Map("origin" -> node)) // send to next node with hinted hand-off
      }
    }

    nodes.take(quorum).map(n => (n, nextLayerBuilder(n))).foreach(n => send(n._1, n._2, context))
    countDownLatch.await()

    Success()
  }

  // TODO add Try when run out of Nodes
  private def nextNode(nodes : List[Node]) : () => Node = {
    val index = new AtomicInteger(quorum)
    def next() = nodes(index.getAndIncrement)
    next
  }
}