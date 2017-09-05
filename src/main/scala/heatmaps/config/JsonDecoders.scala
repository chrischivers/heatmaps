package heatmaps.config

import com.google.maps.model.{LatLng, PlaceType => GooglePlaceType}
import heatmaps.models._
import io.circe.{Decoder, DecodingFailure, HCursor}

import scala.util.{Success, Try}

object JsonDecoders {

  implicit val decodeCity: Decoder[City] = new Decoder[City] {
    final def apply(c: HCursor): Decoder.Result[City] =
      for {
        name <- c.downField("name").as[String]
        defaultViewLat <- c.downField("default-view").downField("lat").as[Double]
        defaultViewLng <- c.downField("default-view").downField("lng").as[Double]
        defaultViewZoom <- c.downField("default-view").downField("zoom").as[Int]
      } yield {
        City(name,
          DefaultView(new LatLng(defaultViewLat, defaultViewLng), defaultViewZoom))
      }
  }

  implicit val decodeCategory: Decoder[Category] = new Decoder[Category] {
    final def apply(c: HCursor): Decoder.Result[Category] =
      for {
        id <- c.downField("id").as[String]
        friendlyName <- c.downField("friendly-name").as[String]
      } yield {
        Category(id, friendlyName, GooglePlaceType.valueOf(id))
      }
  }

  implicit val decodeCompany: Decoder[Company] = new Decoder[Company] {
    final def apply(c: HCursor): Decoder.Result[Company] =
      for {
        id <- c.downField("id").as[String]
        friendlyName <- c.downField("friendly-name").as[String]
        parentCategoryId <-  c.downField("parent-category").as[String]
        searchMatches <-  c.downField("search-matches").as[List[String]]
      } yield {
        Company(id, friendlyName, parentCategoryId, searchMatches)
      }
  }

  val decodeCities = Decoder[List[City]].prepare(_.downField("cities"))
  val decodeCategories = Decoder[List[Category]].prepare(_.downField("categories"))
  val decodeCompanies = Decoder[List[Company]].prepare(_.downField("companies"))

}
