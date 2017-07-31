package heatmaps.web

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.Definitions.{latLngRegions, logger}
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

    case req@GET -> Root / "heatpoints" :? BoundsQueryParamMatcher(bounds) :? PlaceTypeQueryParamMatcher(placeType)=>
      logger.info(s"Servlet handling heatpoints request for bounds $bounds and placeType $placeType")
      val boundsConverted = getBounds(bounds)
      val placeTypeConverted = PlaceType.valueOf(placeType)
      val latLngRegionsInFocus: List[LatLngRegion] =  getLatLngRegionsForLatLngBounds(boundsConverted)
      val jsonStr = Future.sequence(latLngRegionsInFocus.map(latLngRegion => {
        placesDBRetriever.getPlaces(latLngRegion, placeTypeConverted, Some(getBounds(bounds)))
      })).map(x => x.flatten.toSet[Place].map(place => place.latLng).asJson.noSpaces)
      Ok(jsonStr)
    }
}

object HeatmapsServlet extends StrictLogging {

  def getBounds(boundsStr: String): LatLngBounds = {
    val boundsSplit = boundsStr.replaceAll("[() ]", "").split(",").map(_.toDouble)
    LatLngBounds(new LatLng(boundsSplit(0), boundsSplit(1)), new LatLng(boundsSplit(2), boundsSplit(3)))
  }


  def getLatLngRegionsForLatLngBounds(latLngbounds: LatLngBounds): List[LatLngRegion] = {

    def roundDown(n: Double) = Math.floor(n).toInt
    def roundUp(n: Double) = Math.ceil(n).toInt
    logger.info(s"getting regions for latLngBounds: $latLngbounds")

    val southWestBoundary = new LatLng(roundDown(latLngbounds.southwest.lat), roundDown(latLngbounds.southwest.lng))
    val northEastBoundary = new LatLng(roundUp(latLngbounds.northeast.lat), roundUp(latLngbounds.northeast.lng))

    val filteredRegions = latLngRegions.filter(region => {
      region.lat >= southWestBoundary.lat &&
        region.lng >= southWestBoundary.lng &&
        region.lat < northEastBoundary.lat &&
        region.lng < northEastBoundary.lng
    })

    logger.info(s"LatLngRegions obtained: \n ${filteredRegions.mkString("\n")}")

    filteredRegions
  }
}
