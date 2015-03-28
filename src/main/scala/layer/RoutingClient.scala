package layer

import partition.{Node, Versioned, MetaServer}

import scala.util.{Success, Failure, Try}

/**
 * @param nextLayer RPC client to call consistency layer
 */
class RoutingClient(metaServer : MetaServer, nextLayer : Layer) extends Layer {
  override def get(key : String, context : Map[String, AnyRef]) : Try[Iterable[Versioned]] = {
    getFromCoordinator(key, context)
  }

  override def put(key : String, value : Versioned, context : Map[String, AnyRef]) : Try[Unit] = {
    putToCoordinator(key, value, context)
  }

  private def getFromCoordinator(key : String, context : Map[String, AnyRef]) : Try[Iterable[Versioned]] = {
    for(n <- metaServer.getPreferenceList(key)) {
      nextLayer.get(key, Map("coordinator" -> n, "class" -> Node.getClass.getName)) match {
        case Failure(_) =>  // chosen coordinator was down. continue to choose another
        case Success(s) => return Success(s)
      }
    }

    Failure(new RuntimeException("none of servers is available !!"))
  }

  private def putToCoordinator(key : String, value : Versioned, context : Map[String, AnyRef]) : Try[Unit] = {
    for(n <- metaServer.getPreferenceList(key)) {
      nextLayer.put(key, value, Map("coordinator" -> n, "class" -> Node.getClass.getName)) match {
        case Failure(_) =>  // chosen coordinator was down. continue to choose another
        case Success(_) => return Success()
      }
    }

    Failure(new RuntimeException("none of servers is available !!"))
  }
}