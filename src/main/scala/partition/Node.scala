package partition

case class Node (ip : String, port : Int) {
  override def toString() : String = ip + ":" + port
  override def hashCode() : Int = toString().hashCode
  override def equals(that : Any) : Boolean = {
    if(that.isInstanceOf[Node]) {
      val thatNode = that.asInstanceOf[Node]
      thatNode.ip.equals(ip) && thatNode.port.equals(port)
    } else
      false
  }
}