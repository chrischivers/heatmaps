package heatmaps.models

import com.google.maps.model.{PlaceType => GooglePlaceType}

case class PlaceGroup(category: Category, companies: List[Company])

sealed trait PlaceType {
  val id: String
  val friendlyName: String
  override def toString: String = id
}

case class Category(id: String, friendlyName: String, googlePlaceType: GooglePlaceType) extends PlaceType

case class Company(id: String, friendlyName: String, parentCategoryId: String, searchMatches: List[String]) extends PlaceType