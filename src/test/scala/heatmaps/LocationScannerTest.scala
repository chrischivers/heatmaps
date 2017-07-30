package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.LatLngRegion
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._


class LocationScannerTest extends fixture.FunSuite with StrictLogging with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig (
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(placesApiRetriever: PlacesApiRetriever, locationScanner: LocationScanner, placesTable: PlacesTable)

  def withFixture(test: OneArgTest) = {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val db = new PostgresDB(config.dBConfig)
    val placesTable = new PlacesTable(db, PlaceTableSchema(tableName = "placestest"), createNewTable = true)
    val placesDBRetriever = new PlacesDBRetriever(placesTable, config.cacheConfig)
    val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

    val testFixture = FixtureParam(placesApiRetriever, ls, placesTable)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      placesTable.dropTable.futureValue
      db.disconnect.futureValue
    }
  }

  test("Location Scanner should retrieve a list of all places in a given latLngRegion") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT).futureValue

    locationScanResult.size should be > 500
    locationScanResult.size should be < 1000
  }

  test("place retriever results for mid point of a given area should be a subset of location scanner results for the entire area") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT).futureValue

    val midPointResults = f.placesApiRetriever.getPlaces(new LatLng(45.5, 25.5), 1000, PlaceType.RESTAURANT).futureValue

    midPointResults.size should be < locationScanResult.size

    val locationScanResultPlaceIds = locationScanResult.map(_.placeId)

    midPointResults.length should be < locationScanResultPlaceIds.length
    midPointResults.map(_.placeId).foreach(placeId => {
      locationScanResultPlaceIds should contain(placeId)
    })
  }

  test("Location scan with wide radius should return same result set as location scan with small radius") { f =>

    val latLngRegion = LatLngRegion(45, 25)

    val scanResultsLargeRadius = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000,  PlaceType.RESTAURANT).futureValue
    val scanResultsLargeRadiusPlaceIds = scanResultsLargeRadius.map(result => result.placeId)

    val scanResultsSmallRadius = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 50000,  PlaceType.RESTAURANT).futureValue
    val scanResultsSmallRadiusPlaceIds = scanResultsSmallRadius.map(result => result.placeId)

    scanResultsSmallRadiusPlaceIds.foreach(placeId => scanResultsLargeRadiusPlaceIds should contain (placeId))
  }
}
