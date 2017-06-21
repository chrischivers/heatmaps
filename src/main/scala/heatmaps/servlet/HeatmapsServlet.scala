package heatmaps.servlet

import com.google.maps.model.LatLng
import com.typesafe.scalalogging.StrictLogging
import heatmaps.{CityDefinitions, LatLngBounds, Main}
import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global

object BoundsQueryParamMatcher extends QueryParamDecoderMatcher[String]("bounds")

case class PlacePositions(placeType: String, latLngList: List[LatLng])

object HeatmapsServlet extends StrictLogging {

  implicit val latLngEncoder: Encoder[LatLng] =
    Encoder.forProduct2("lat", "lng")(u =>
      (u.lat, u.lng))
  implicit val placePositionsEncoder: Encoder[PlacePositions] = deriveEncoder

  val service = HttpService {
    case req@GET -> Root / "map" =>
      logger.info(s"Servlet handling map request")
      Ok(html.map())
    case req@GET -> Root / "heatpoints" :? BoundsQueryParamMatcher(bounds) =>
      logger.info(s"Servlet handling heatpoints request for bounds $bounds")
      val boundsConverted = getBounds(bounds)
      val cityInFocus = CityDefinitions.getCityForLatLngBounds(boundsConverted)
      cityInFocus.map { city =>
        Main.placesDBRetriever.getPlaces(city, Some(getBounds(bounds)))
          .map(_.groupBy(_.placeType)
            .map { case (placeType, positions) => PlacePositions(placeType, positions.map(_.latLng)) }
            .asJson.noSpaces)
      } match {
        case Some(jsonStr) =>
          logger.info(s"Records found for city $cityInFocus, returning json String")
          Ok(jsonStr)
        case None =>
          logger.info(s"No city found for bounds $boundsConverted. Returning empty json")
          Ok("{}")
      }
    }

  def getBounds(boundsStr: String): LatLngBounds = {
    val boundsSplit = boundsStr.replaceAll("[() ]", "").split(",").map(_.toDouble)
    LatLngBounds(new LatLng(boundsSplit(0), boundsSplit(1)), new LatLng(boundsSplit(2), boundsSplit(3)))
  }
}