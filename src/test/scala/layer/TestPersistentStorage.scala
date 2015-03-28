package layer

import com.twitter.util.CountDownLatch
import org.junit.Test
import partition._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._
import scala.util.{Success, Failure}

class TestPersistentStorage {
  @Test
  def testLocalPersistentStorage = {
    val server = new PersistentStorageServer(9001, "/Users/morefree/Developments/nosql-workspace/mapdb/testDB", "testColl", -1)
    val latch = new CountDownLatch(1)

    Future {
      latch.await()
      server.stop
    }

    Future {
      server.start
    }

    Thread.sleep(1000)  // wait for server to start
    val client = new PersistentStorageClient("127.0.0.1", 9001)
    val vectorClock = new VectorClock(Map("s1" -> 1, "s2" -> 1, "s3" -> 3))
    val contents = "all content".getBytes
    client.put("some", Versioned(vectorClock, contents), Map[String, AnyRef]())

    client.getAsync("some", Map[String, AnyRef]()) onSuccess {
      case Success(r) => {
        r.foreach(v => println("version = " + v.version + ", value = " + new String(v.value)))
        latch.countDown()
      }
      case Failure(_) => {
        latch.countDown()
      }
    }

    Thread.sleep(1000) // wait for getAsync to complete
  }
}
