package heatmaps

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.LatLngRegion
import org.scalatest.Matchers._
import org.scalatest.RecoverMethods._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DBCacheTest extends fixture.FunSuite with ScalaFutures with StrictLogging {

  val config = ConfigLoader.defaultConfig
  logger.info("DB NAME: " + config.dBConfig.dbName)

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

  test("places fetched from cache are same as those fetched directly from DB") { f =>

    val placeType = PlaceType.RESTAURANT
    val placesDBRetriever = new PlacesDBRetriever(f.placesTable, config.cacheConfig)

    val latLngRegion = LatLngRegion(45, 25)
    (for {
      locationScanResult <- f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT)
       _ <- f.placesTable.insertPlaces(locationScanResult, latLngRegion, PlaceType.RESTAURANT)
    } yield ()).futureValue
    val resultsFromDB = f.placesTable.getPlacesForLatLngRegion(latLngRegion, placeType).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(latLngRegion, placeType).futureValue

    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.placesTable.dropTable.futureValue
    val resultsFromCacheAgain = placesDBRetriever.getPlaces(latLngRegion, placeType).futureValue
    resultsFromCacheAgain.map(_.placeId) should contain theSameElementsAs resultsFromDB.map(_.placeId)
  }

  test("places are fetched again from db when cached records expire") { f =>

    val placeType = PlaceType.RESTAURANT
    val placesDBRetriever = new PlacesDBRetriever(f.placesTable, config.cacheConfig.copy(timeToLive = 5 seconds))

    val latLngRegion = LatLngRegion(45, 25)
    (for {
      locationScanResult <- f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType)
      _ <- f.placesTable.insertPlaces(locationScanResult, latLngRegion, placeType)
    } yield ()).futureValue

    val resultsFromDB = f.placesTable.getPlacesForLatLngRegion(latLngRegion, placeType).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(latLngRegion, placeType).futureValue

    resultsFromDB.size should be > 0
    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.placesTable.dropTable.futureValue
    Thread.sleep(5000)
    recoverToSucceededIf[GenericDatabaseException] {
      placesDBRetriever.getPlaces(latLngRegion, placeType)
    }
  }
}
