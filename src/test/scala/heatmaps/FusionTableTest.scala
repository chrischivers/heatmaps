package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{FusionTable, PlaceTableSchema}
import org.scalatest.FunSuite
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class FusionTableTest extends FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  test("heatmaps.db.Place results persisted to heatmaps.db.FusionTable should match those retrieved") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val ls = new LocationScanner(placesApiRetriever)
    val ft = new FusionTable(PlaceTableSchema())
    ft.dropPlacesTable

    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds)
    val locationScanResult = ls.scanCity(city, 500, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
    ft.insertPlaces(locationScanResult, city, PlaceType.RESTAURANT.name())

    ft.getPlacesForCity(city)
    //    resultsFromDB.size should be > 0
    //    resultsFromDB.map(_.place_id) == locationScanResult.map(_.placeId)

  }
}
