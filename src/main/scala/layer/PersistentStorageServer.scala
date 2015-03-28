package layer

// specific implementations binding with thrift RPC. start separately on each node. TODO decouple with Thrift
class PersistentStorageServer(port : Int, db : String, coll : String, interval : Int) {
  private val server = new rpc.thrift.PersistentStorageServer(port, db, coll, interval)

  def start = server.start() /* it will block the thread */
  def stop = server.stop()
}
