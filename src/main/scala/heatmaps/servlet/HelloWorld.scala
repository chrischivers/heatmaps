package heatmaps.servlet

import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

object HelloWorld {
  val service = HttpService {
    case req @ GET -> Root =>
      Ok(html.index("HELLO CHRIS"))
  }

}
