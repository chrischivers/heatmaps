package heatmaps.servlet

import com.google.maps.model.LatLng
import heatmaps.{LatLngBounds, Main}
import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s._
import org.http4s.circe._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global

object BoundsQueryParamMatcher extends QueryParamDecoderMatcher[String]("bounds")

case class PlacePositions(placeType: String, latLngList: List[LatLng])

object HelloWorld {

  implicit val latLngEncoder: Encoder[LatLng] =
    Encoder.forProduct2("lat", "lng")(u =>
      (u.lat, u.lng))
  implicit val placePositionsEncoder: Encoder[PlacePositions] = deriveEncoder

//  val places = Await.result(Main.placesDBRetriever.getPlaces(Main.london), 2 minute)
  val service = HttpService {
    case req@GET -> Root / "map" =>
      Ok(html.map())
    case req@GET -> Root / "heatpoints" :? BoundsQueryParamMatcher(bounds) =>
      val jsonStr = Main.placesDBRetriever.getPlaces(Main.london, Some(getBounds(bounds)))
          .map(_.groupBy(_.placeType)
          .map{ case (placeType, positions) => PlacePositions(placeType, positions.map(_.latLng))}
          .asJson.noSpaces)
      Ok(jsonStr)
    }

  def getBounds(boundsStr: String): LatLngBounds = {
    val boundsSplit = boundsStr.replaceAll("[() ]", "").split(",").map(_.toDouble)
    LatLngBounds(new LatLng(boundsSplit(0), boundsSplit(1)), new LatLng(boundsSplit(2), boundsSplit(3)))
  }
}