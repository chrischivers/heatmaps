package heatmaps.models

import com.google.maps.model.{PlaceType => GooglePlaceType}
import heatmaps.models.Category.{Cafe, Lodging, Restaurant}

case class PlaceGroup(category: Category, companies: List[Company])

sealed trait PlaceType {
  val name: String
  override def toString: String = name
}

sealed trait Category extends PlaceType {
  val googlePlaceType: GooglePlaceType
  lazy val name: String = googlePlaceType.name()
}

sealed trait Company extends PlaceType {
  val searchMatches: List[String]
  val placeCategory: Category
}

object Category {

  val allCategories = List(Restaurant, Cafe, University, TrainStation, ShoppingMall, Lodging, Store, School, Park, Museum, Bar, Bank)

  def fromString(str: String): Option[Category] = {
    allCategories.find(_.name == str.toUpperCase)
  }

  case object Restaurant extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.RESTAURANT
  }

  case object Cafe extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.CAFE
  }

  case object University extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.UNIVERSITY
  }

  case object TrainStation extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.TRAIN_STATION
  }

  case object ShoppingMall extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.SHOPPING_MALL
  }

  case object Lodging extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.LODGING
  }

  case object Store extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.STORE
  }

  case object School extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.SCHOOL
  }

  case object Park extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.PARK
  }

  case object Museum extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.MUSEUM
  }

  case object Bar extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.BAR
  }

  case object Bank extends Category {
    override val googlePlaceType: GooglePlaceType = GooglePlaceType.BANK
  }
}




object Company {

  val allCompanies = List(McDonalds, Starbucks, BurgerKing, Travelodge)

  def fromString(str: String): Option[Company] = {
    allCompanies.find(_.name == str.toUpperCase)
  }

  case object McDonalds extends Company {
    override val name: String = "MCDONALDS"
    override val searchMatches: List[String] = List("McDonalds", "McDonald's")
    override val placeCategory: Category  = Restaurant
  }

  case object BurgerKing extends Company {
    override val name: String = "BURGERKING"
    override val searchMatches: List[String] = List("Burger King", "BurgerKing")
    override val placeCategory: Category = Restaurant
  }

  case object Starbucks extends Company {
    override val name: String = "STARBUCKS"
    override val searchMatches: List[String] = List("Starbuck")
    override val placeCategory: Category = Cafe
  }

  case object Travelodge extends Company {
    override val name: String = "TRAVELODGE"
    override val searchMatches: List[String] = List("Travelodge")
    override val placeCategory: Category = Lodging
  }
}
