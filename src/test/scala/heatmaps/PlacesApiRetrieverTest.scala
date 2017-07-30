package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import googleutils.SphericalUtil
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.LatLngRegion
import org.scalatest.{FunSuite, fixture}
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration._

class PlacesApiRetrieverTest extends fixture.FunSuite with StrictLogging with ScalaFutures {

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
    val placesDBRetriever = new PlacesDBRetriever(placesTable, config.cacheConfig)
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


  test("Place Retriever should retrieve a list of places for a given location, falling within radius") { f =>

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT).futureValue
    f.placesTable.insertPlaces(locationScanResult, latLngRegion, PlaceType.RESTAURANT).futureValue

    val searchLocation = new LatLng(45.4, 25.5)
    val radius = 1000
    val results1 = f.placesApiRetriever.getPlaces(searchLocation, radius, PlaceType.RESTAURANT).futureValue
    results1.foreach(result => {
      val placeLocation = result.geometry.location
      SphericalUtil.computeDistanceBetween(searchLocation, placeLocation).toInt should be <= radius
    })
  }
}