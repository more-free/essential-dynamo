package layer

import partition.Versioned
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.Try

trait Layer {
  def get(key : String, context : Map[String, AnyRef]) : Try[Iterable[Versioned]]
  def put(key : String, value : Versioned, context : Map[String, AnyRef]) : Try[Unit]
  def getAsync(key : String, context : Map[String, AnyRef]) : Future[Try[Iterable[Versioned]]] =
    Future { get(key, context) }
  def putAsync(key : String, value : Versioned, context : Map[String, AnyRef]) : Future[Try[Unit]] =
    Future { put(key, value, context) }
}