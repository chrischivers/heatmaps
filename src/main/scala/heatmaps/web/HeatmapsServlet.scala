package heatmaps.web

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.Definitions.{allLatLngRegions, logger}
import heatmaps.models.{DefaultView, LatLngBounds, LatLngRegion, Place}
import heatmaps.config.Definitions
import io.circe.Encoder
import io.circe.syntax._
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object BoundsQueryParamMatcher extends QueryParamDecoderMatcher[String]("bounds")
object PlaceTypeQueryParamMatcher extends QueryParamDecoderMatcher[String]("placeType")
object CityDefaultViewQueryParamMatcher extends QueryParamDecoderMatcher[String]("city")
object ZoomQueryParamMatcher extends QueryParamDecoderMatcher[String]("zoom")

case class Density(value: Double)

class HeatmapsServlet(placesDBRetriever: PlacesRetriever) extends StrictLogging {

  import HeatmapsServlet._

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

    case req@GET -> Root / "cities" =>
      logger.info("Servlet handling cities request")
      Ok(Definitions.cities.map(_.name).asJson.noSpaces)

    case req@GET -> Root / "placetypes" => {}
      logger.info("Servlet handling placetypes request")
      Ok(Definitions.placeTypes.map(_.name()).asJson.noSpaces)

    case req@GET -> Root / "heatpoints"
      :? BoundsQueryParamMatcher(bounds)
      :? PlaceTypeQueryParamMatcher(placeType)
      :? ZoomQueryParamMatcher(zoom) =>
      logger.info(s"Servlet handling heatpoints request for bounds $bounds and placeType $placeType")
      val boundsConverted = getBounds(bounds)
      val density = zoomToDensity(zoom.toInt)
      val placeTypeConverted = PlaceType.valueOf(placeType)
      val latLngRegionsInFocus: List[LatLngRegion] =  Definitions.getLatLngRegionsForLatLngBounds(boundsConverted)
      val jsonStr = placesDBRetriever.getPlaces(latLngRegionsInFocus, placeTypeConverted, Some(getBounds(bounds)), None) //Ignoring density for now
        .map(x => x.toSet[Place].map(place => place.latLng).asJson.noSpaces)
      Ok(jsonStr)
    }
}

object HeatmapsServlet extends StrictLogging {

  def getBounds(boundsStr: String): LatLngBounds = {
    val boundsSplit = boundsStr.replaceAll("[() ]", "").split(",").map(_.toDouble)
    LatLngBounds(new LatLng(boundsSplit(0), boundsSplit(1)), new LatLng(boundsSplit(2), boundsSplit(3)))
  }

  //Returns the density (%) of places required based on zoom level
  def zoomToDensity(zoom: Int): Density = {
    val minZoom = 1
    val maxZoom = 20
    val normalisedZoom = if (zoom < minZoom) minZoom else if (zoom > maxZoom) maxZoom else zoom
    Density(normalisedZoom * (100.0 / maxZoom))
  }
}
