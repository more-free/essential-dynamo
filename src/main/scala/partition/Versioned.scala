package partition

case class Versioned(val version : VectorClock, val value : Array[Byte]) {
  def reconcile(that : Versioned) : List[Versioned] = {
    if(version.happenBefore(that.version)) List(that)
    else if(that.version.happenBefore(version)) List(this)
    else List(this, that)
  }

  def happenBefore(that : Versioned) = version.happenBefore(that.version)

  override def equals(that : Any) =
    that.isInstanceOf[Versioned] && that.asInstanceOf[Versioned].version.equals(version)

  override def hashCode : Int = version.hashCode
  override def toString : String = "version = " + version.toString + ", value = " + new String(value)
}
