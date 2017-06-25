package heatmaps.servlet

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.{DefaultView, Definitions, LatLngBounds, PlacesDBRetriever}
import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global

object BoundsQueryParamMatcher extends QueryParamDecoderMatcher[String]("bounds")
object PlaceTypeQueryParamMatcher extends QueryParamDecoderMatcher[String]("placeType")
object CityDefaultViewQueryParamMatcher extends QueryParamDecoderMatcher[String]("city")

//case class PlacePositions(placeType: String, latLngList: List[LatLng])

class HeatmapsServlet(placesDBRetriever: PlacesDBRetriever) extends StrictLogging {

  implicit val latLngEncoder: Encoder[LatLng] =
    Encoder.forProduct2("lat", "lng")(u =>
      (u.lat, u.lng))

  implicit val defaultViewEncoder: Encoder[DefaultView] =
    Encoder.forProduct3("lat", "lng", "zoom")(u =>
      (u.latLng.lat, u.latLng.lng, u.zoom))

  val service = HttpService {
    case req@GET -> Root / "map" =>
      logger.info(s"Servlet handling map request")
      Ok(html.map())
    case req@GET -> Root / "defaultView" :? CityDefaultViewQueryParamMatcher(city) =>
      logger.info(s"Servlet handling defaultView request")
      Definitions.cities.find(_.name == city) match {
        case Some(cityDef) =>
          val jsonStr = cityDef.defaultView.asJson.noSpaces
          logger.info(s"Found city $city in DB, returning json string $jsonStr")
          Ok(jsonStr)
        case None =>
          logger.info(s"Not found city $city in DB, returning empty string")
          Ok("{}")
      }
    case req@GET -> Root / "heatpoints" :? BoundsQueryParamMatcher(bounds) :? PlaceTypeQueryParamMatcher(placeType)=>
      logger.info(s"Servlet handling heatpoints request for bounds $bounds and placeType $placeType")
      val boundsConverted = getBounds(bounds)
      val placeTypeConverted = PlaceType.valueOf(placeType)
      val cityInFocus = Definitions.getCityForLatLngBounds(boundsConverted)
      cityInFocus.map { city =>
        placesDBRetriever.getPlaces(city, placeTypeConverted, Some(getBounds(bounds)))
          .map(_.map(place => place.latLng)
            .asJson.noSpaces)
      } match {
        case Some(jsonStr) =>
          logger.info(s"Records found for city $cityInFocus , returning json String")
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