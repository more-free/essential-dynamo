package util

/**
 * Created by morefree on 12/7/14.
 */
object Config {
  def loadProperties(file : String) = {
    val prop = new java.util.Properties()
    prop.load(this.getClass.getClassLoader.getResourceAsStream(file))
    prop
  }
}
