package heatmaps.models

import com.google.maps.model.PlaceType

case class PlaceGroup(placeType: PlaceType, subTypes: List[PlaceSubType])

sealed trait PlaceSubType {
  val name: String
  val searchMatches: List[String]
  val parentType: PlaceType
  override def toString: String = name
}

case object McDonalds extends PlaceSubType {
  override val name: String = "MCDONALDS"
  override val searchMatches: List[String] = List("McDonalds", "McDonald's")
  override val parentType: PlaceType = PlaceType.RESTAURANT
}

case object Starbucks extends PlaceSubType {
  override val name: String = "STARBUCKS"
  override val searchMatches: List[String] = List("Starbuck")
  override val parentType: PlaceType = PlaceType.CAFE
}

object PlaceSubType {
  def fromString(str: String): Option[PlaceSubType] = {
    List(McDonalds, Starbucks).find(_.name == str.toUpperCase)
  }
}