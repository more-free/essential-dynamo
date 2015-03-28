package partition

import util.HashFunction


class MetaServer {
  val config = util.Config.loadProperties("routing.properties")
  private val md5HashRing = new ConsistentHash[String, Node](HashFunction.md5, config.getProperty("virtual_nodes").toInt)

  def addServer(server : Node) = md5HashRing.add(server.toString(), server)
  def removeServer(server : Node) = md5HashRing.remove(server.toString())
  def getServer(key : String) = md5HashRing.get(key)
  def getPreferenceList(key : String) : List[Node] = {
    val replica = config.getProperty("replica").toInt
    md5HashRing.getAll(key).take(replica)
  }
  def getAll(key : String) : List[Node] = md5HashRing.getAll(key)
}