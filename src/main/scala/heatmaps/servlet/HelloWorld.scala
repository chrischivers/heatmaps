package heatmaps.servlet

import com.google.maps.model.LatLng
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

object HelloWorld {
  val service = HttpService {
    case req @ GET -> Root / "map" =>
      Ok(html.map(new LatLng(0,0)))
  }

}
