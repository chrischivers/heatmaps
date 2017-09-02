package heatmaps.web

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.{Definitions, MapsConfig}
import heatmaps.models._
import io.circe.Encoder
import io.circe.generic.semiauto._
import io.circe.syntax._
import org.http4s._
import org.http4s.dsl._
import org.http4s.twirl._

import scala.concurrent.ExecutionContext.Implicits.global

object BoundsQueryParamMatcher extends QueryParamDecoderMatcher[String]("bounds")
object CategoryQueryParamMatcher extends QueryParamDecoderMatcher[String]("category")
object PlaceSubTypeQueryParamMatcher extends QueryParamDecoderMatcher[String]("placeSubType")
object CityDefaultViewQueryParamMatcher extends QueryParamDecoderMatcher[String]("city")
object ZoomQueryParamMatcher extends QueryParamDecoderMatcher[String]("zoom")

class HeatmapsServlet(placesDBRetriever: PlacesRetriever, mapsConfig: MapsConfig) extends StrictLogging {

  import HeatmapsServlet._

  implicit val latLngEncoder: Encoder[LatLng] =
    Encoder.forProduct2("lat", "lng")(u =>
      (u.lat, u.lng))

  implicit val defaultViewEncoder: Encoder[DefaultView] =
    Encoder.forProduct3("lat", "lng", "zoom")(u =>
      (u.latLng.lat, u.latLng.lng, u.zoom))

  implicit val placeCategoryEncoder: Encoder[Category] =
    Encoder.forProduct1("name")(u => u.name)

  implicit val companyEncoder: Encoder[Company] =
    Encoder.forProduct1("name")(u => u.name)

  implicit val placeGroupEncoder: Encoder[PlaceGroup] = deriveEncoder[PlaceGroup]

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

    case req@GET -> Root / "placegroups" =>
      logger.info("Servlet handling placegroups request")
      Ok(Definitions.placeGroups.asJson.noSpaces)

    case req@GET -> Root / "heatpoints"
      :? BoundsQueryParamMatcher(bounds)
      :? CategoryQueryParamMatcher(category)
      :? ZoomQueryParamMatcher(zoom) =>
      logger.info(s"Servlet handling heatpoints request for bounds $bounds and category $category")
      val boundsConverted = getBounds(bounds)
      val categoryConverted = Category.fromString(category)
      val zoomCorrected =
        if (zoom.toInt > mapsConfig.maxZoom) mapsConfig.maxZoom
        else if (zoom.toInt < mapsConfig.minZoom) mapsConfig.minZoom
        else zoom.toInt

      categoryConverted.fold(NotFound()) { category =>
        val latLngRegionsInFocus: List[LatLngRegion] = Definitions.getLatLngRegionsForLatLngBounds(boundsConverted)
        val jsonStr = placesDBRetriever.getPlaces(latLngRegionsInFocus, category, Some(getBounds(bounds)), Some(zoomCorrected)) //Ignoring density for now
          .map(x => x.toSet[Place].map(place => place.latLng).asJson.noSpaces)
        Ok(jsonStr)
      }

//    case req@GET -> Root / "heatpoints"
//      :? BoundsQueryParamMatcher(bounds)
//      :? PlaceTypeQueryParamMatcher(placeType)
//      :? PlaceSubTypeQueryParamMatcher(subType)
//      :? ZoomQueryParamMatcher(zoom) =>
//      logger.info(s"Servlet handling heatpoints request for bounds $bounds, placeType $placeType, placeSubType $subType")
//      val boundsConverted = getBounds(bounds)
//      val placeTypeConverted = PlaceType.valueOf(placeType)
//      val subTypeOpt: Option[Company] = if(subType.toUpperCase() == "ALL") None else Company.fromString(subType)
//      val zoomCorrected =
//        if (zoom.toInt > mapsConfig.maxZoom) mapsConfig.maxZoom
//        else if (zoom.toInt < mapsConfig.minZoom) mapsConfig.minZoom
//        else zoom.toInt
//      //TODO
//      val latLngRegionsInFocus: List[LatLngRegion] =  Definitions.getLatLngRegionsForLatLngBounds(boundsConverted)
//      val jsonStr = placesDBRetriever.getPlaces(latLngRegionsInFocus, placeTypeConverted, subTypeOpt, Some(getBounds(bounds)), Some(zoomCorrected)) //Ignoring density for now
//        .map(x => x.toSet[Place].map(place => place.latLng).asJson.noSpaces)
//      Ok(jsonStr)
    }
}

object HeatmapsServlet extends StrictLogging {

  def getBounds(boundsStr: String): LatLngBounds = {
    val boundsSplit = boundsStr.replaceAll("[() ]", "").split(",").map(_.toDouble)
    LatLngBounds(new LatLng(boundsSplit(0), boundsSplit(1)), new LatLng(boundsSplit(2), boundsSplit(3)))
  }
}
