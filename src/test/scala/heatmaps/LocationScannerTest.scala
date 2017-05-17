package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import org.scalatest.FunSuite
import org.scalatest.Matchers._


class LocationScannerTest extends FunSuite {

  val config = ConfigLoader.defaultConfig

  test("Location Scanner should retrieve a list of all places in a given location. ") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val ls = new LocationScanner(placesApiRetriever)
    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds)
    val locationScanResult = ls.scanCity(city, 500, PlaceType.RESTAURANT)

    locationScanResult.size should be > 200
  }

  test("place retriever results for mid point of a given area should be a subset of location scanner results for the entire area") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val ls = new LocationScanner(placesApiRetriever)
    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds)
    val locationScanResult = ls.scanCity(city, 500, PlaceType.RESTAURANT)

    val midPointResults = placesApiRetriever.getPlaces(new LatLng(51.513755, -0.134346), 200, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = true)

    val locationScanResultPlaceIds = locationScanResult.map(_.placeId)

    midPointResults.length should be < locationScanResultPlaceIds.length
    midPointResults.map(_.placeId).foreach(placeId => {
      locationScanResultPlaceIds should contain(placeId)
    })
  }

  test("Location scan with wide radius should return same result set as location scan with small radius") {

    val southWest = new LatLng(51.510640, -0.140641)
    val northEast = new LatLng(51.516890, -0.131972)
    val latLngBounds = LatLngBounds(southWest, northEast)
    val london = City("London", latLngBounds)

    val placesApiRetriever = new PlacesApiRetriever(config)
    val ls = new LocationScanner(placesApiRetriever)

    val scanResultsLargeRadius = ls.scanCity(london, 500,  PlaceType.RESTAURANT)
    val scanResultsLargeRadiusPlaceIds = scanResultsLargeRadius.map(result => result.placeId)

    val scanResultsSmallRadius = ls.scanCity(london, 250,  PlaceType.RESTAURANT)
    val scanResultsSmallRadiusPlaceIds = scanResultsSmallRadius.map(result => result.placeId)

    scanResultsSmallRadiusPlaceIds.foreach(placeId => scanResultsLargeRadiusPlaceIds should contain (placeId))
  }
}
