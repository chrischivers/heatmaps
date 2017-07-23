package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import googleutils.SphericalUtil
import org.scalatest.FunSuite
import org.scalatest.Matchers._

class PlaceRetrieverTest extends FunSuite {

  val config = ConfigLoader.defaultConfig

  //TODO test for latLngRegion that has no places

  test("heatmaps.db.Place Retriever should retrieve a list of places for a given location") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val searchLocation = new LatLng(51.512500, -0.114691)
    val radius = 500
    val results = placesApiRetriever.getPlaces(searchLocation, radius, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
    results.foreach(result => {
      val placeLocation = result.geometry.location
      SphericalUtil.computeDistanceBetween(searchLocation, placeLocation).toInt should be <= radius
    })
  }
}
