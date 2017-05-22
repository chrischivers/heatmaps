package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{FusionTable, PlaceTableSchema}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.duration._

class FusionTableTest extends fixture.FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(10 minutes),
    interval = scaled(1 seconds)
  )

  case class FixtureParam(placesApiRetriever: PlacesApiRetriever, locationScanner: LocationScanner, fusionTable: FusionTable, requestsPerMinute: Double)

  def withFixture(test: OneArgTest) = {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val requestsPerMinute = 29.0
    val locationScanner = new LocationScanner(placesApiRetriever)
    val fusionTable = new FusionTable(config.fusionDBConfig, PlaceTableSchema(), requestsPerMinute, recreateTableIfExists = true)
    val testFixture = FixtureParam(placesApiRetriever, locationScanner, fusionTable, requestsPerMinute)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      //fusionTable.dropPlacesTable
    }
  }


  test("heatmaps.db.Place results persisted to FusionTable should match those retrieved") { f =>
    {

      val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
      val city = City("TestCity", latLngBounds)

      val locationScanResult = f.locationScanner.scanCity(city, 500, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
      f.fusionTable.insertPlaces(locationScanResult, city, PlaceType.RESTAURANT.name()).futureValue

      val results = f.fusionTable.getPlacesForCity(city).futureValue
      results.size should be > 0
      results.map(_.placeId) should equal (locationScanResult.map(_.placeId))
      results.foreach(returnedRes => {
        val locScanResultForPlace = locationScanResult.find(res => res.placeId == returnedRes.placeId).get
        locScanResultForPlace.geometry.location.lat should equal (returnedRes.latLng.lat)
        locScanResultForPlace.geometry.location.lng should equal (returnedRes.latLng.lng)
      })

    }
  }

  test ("requests per minute throttle is adhered to") { f =>
    {
      val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
      val city = City("TestCity", latLngBounds)

      val locationScanResultExtract = f.locationScanner.scanCity(city, 500, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false).take(f.requestsPerMinute.toInt)
      val startTime = System.currentTimeMillis()
      f.fusionTable.insertPlaces(locationScanResultExtract, city, PlaceType.RESTAURANT.name()).futureValue
      val finishTime = System.currentTimeMillis()

      (finishTime - startTime) should be >= (60 * 1000).toLong
    }
  }
}
