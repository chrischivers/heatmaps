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

case object BurgerKing extends PlaceSubType {
  override val name: String = "BURGERKING"
  override val searchMatches: List[String] = List("Burger King", "BurgerKing")
  override val parentType: PlaceType = PlaceType.RESTAURANT
}

case object Starbucks extends PlaceSubType {
  override val name: String = "STARBUCKS"
  override val searchMatches: List[String] = List("Starbuck")
  override val parentType: PlaceType = PlaceType.CAFE
}

case object Travelodge extends PlaceSubType {
  override val name: String = "TRAVELODGE"
  override val searchMatches: List[String] = List("Travelodge")
  override val parentType: PlaceType = PlaceType.LODGING
}


object PlaceSubType {

  val allSubTypes = List(McDonalds, Starbucks, BurgerKing, Travelodge)

  def fromString(str: String): Option[PlaceSubType] = {
    allSubTypes.find(_.name == str.toUpperCase)
  }
}