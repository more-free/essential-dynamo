package layer

import java.nio.ByteBuffer
import partition.{VectorClock, Versioned}
import scala.util.{Failure, Success, Try}

// simple implementations bound to thrift RPC. TODO decouple with Thrift
class PersistentStorageClient(host : String, port : Int) extends Layer {
  private val client = new rpc.thrift.PersistentStorageClient(host, port)

  override def get(key : String, context : Map[String, AnyRef]) : Try[Iterable[Versioned]] = {
    try {
      Success(toVersionedList(client.get(key)))
    } catch {
      case e : Exception => Failure(e)
    }
  }

  override def put(key : String, versioned : Versioned, context : Map[String, AnyRef]) : Try[Unit] = {
    try {
      if(context.contains("origin")) {
        val origin = context("origin").asInstanceOf[partition.Node]
        Success(client.hintedHandoff(key, toThriftVersioned(versioned), origin.ip, origin.port))
      } else
        Success(client.put(key, toThriftVersioned(versioned)))
    } catch {
      case e : Exception => Failure(e)
    }
  }

  private def toVersionedList(list : java.util.List[rpc.thrift.Versioned]) : Iterable[Versioned] = {
    import scala.collection.JavaConverters._
    list.asScala.map(v => Versioned(VectorClock.createFromJson(v.getVersion), v.getValue))
  }

  private def toThriftVersioned(versioned : Versioned) =
    new rpc.thrift.Versioned(ByteBuffer.wrap(versioned.value), versioned.version.toString)
}
