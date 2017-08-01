package heatmaps

import com.google.maps.model.PlaceType
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.LatLngRegion
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PlacesDBTest extends fixture.FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(placesApiRetriever: PlacesApiRetriever, locationScanner: LocationScanner, placesTable: PlacesTable)

  def withFixture(test: OneArgTest) = {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val db = new PostgresDB(config.dBConfig)
    val placesTable = new PlacesTable(db, PlaceTableSchema(tableName = "placestest"), createNewTable = true)
    val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig)
    val locationScanner = new LocationScanner(placesApiRetriever, placesDBRetriever)

    val testFixture = FixtureParam(placesApiRetriever, locationScanner, placesTable)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      placesTable.dropTable.futureValue
      db.disconnect.futureValue
    }
  }

  test("heatmaps.db.Place results persisted to DB should match those retrieved") { f =>

    val placeType1 = PlaceType.RESTAURANT
    val placeType2 = PlaceType.LODGING

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult1 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType1).futureValue
    f.placesTable.insertPlaces(locationScanResult1, latLngRegion, placeType1).futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType2).futureValue
    f.placesTable.insertPlaces(locationScanResult2, latLngRegion, placeType2).futureValue

    val resultsFromDB1 = f.placesTable.getPlacesForLatLngRegion(latLngRegion, placeType1).futureValue
    resultsFromDB1.size should be > 0
    resultsFromDB1.map(_.placeId) == locationScanResult1.map(_.placeId)
    resultsFromDB1.map(_.placeName) == locationScanResult1.map(_.name)

    val resultsFromDB2 = f.placesTable.getPlacesForLatLngRegion(latLngRegion, placeType2).futureValue
    resultsFromDB2.size should be > 0
    resultsFromDB2.map(_.placeId) == locationScanResult2.map(_.placeId)
    resultsFromDB2.map(_.placeName) == locationScanResult2.map(_.name)
  }
}
