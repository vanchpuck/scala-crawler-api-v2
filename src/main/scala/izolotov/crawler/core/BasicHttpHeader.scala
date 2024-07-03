package izolotov.crawler.core

object BasicHttpHeader {
  def apply(name: String, value: String): BasicHttpHeader = new BasicHttpHeader(name, value)
}

class BasicHttpHeader(val name: String, val value: String) extends HttpHeader {
  if (Option(name).isEmpty) throw new IllegalArgumentException("Header name can't be empty")

  override def toString: String = s"$name: $value"

  override def equals(obj: Any): Boolean = {
    obj match {
      case header: BasicHttpHeader => this.name == header.name && this.value == header.value
      case _ => false
    }
  }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + name.hashCode;
    result = prime * result + Option(value).map(v => v.hashCode()).getOrElse(0)
    result
  }
}
