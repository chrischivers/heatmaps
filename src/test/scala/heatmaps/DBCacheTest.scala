package heatmaps

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.google.maps.model.{LatLng, PlaceType}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.db.{PlaceTableSchema, PostgresqlDB}
import org.scalatest.FunSuite
import org.scalatest.RecoverMethods._
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.fixture

class DBCacheTest extends fixture.FunSuite with ScalaFutures with StrictLogging {

  val config = ConfigLoader.defaultConfig
  logger.info("DB NAME: " + config.postgresDBConfig.dbName)

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(placesApiRetriever: PlacesApiRetriever, locationScanner: LocationScanner, db: PostgresqlDB)

  def withFixture(test: OneArgTest) = {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(tableName = "placestest"), recreateTableIfExists = true)
    val placesDBRetriever = new PlacesDBRetriever(db, config.cacheConfig)
    val locationScanner = new LocationScanner(placesApiRetriever, placesDBRetriever)

    val testFixture = FixtureParam(placesApiRetriever, locationScanner, db)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      db.disconnectFromDB.futureValue
    }
  }

  test("places fetched from cache are same as those fetched directly from DB") { f =>

    val placeType = PlaceType.RESTAURANT
    val placesDBRetriever = new PlacesDBRetriever(f.db, config.cacheConfig)

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
    f.db.insertPlaces(locationScanResult, latLngRegion, PlaceType.RESTAURANT).futureValue

    val resultsFromDB = f.db.getPlacesForLatLngRegion(latLngRegion, placeType).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(latLngRegion, placeType).futureValue

    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.db.dropPlacesTable.futureValue
    val resultsFromCacheAgain = placesDBRetriever.getPlaces(latLngRegion, placeType).futureValue
    resultsFromCacheAgain.map(_.placeId) should contain theSameElementsAs resultsFromDB.map(_.placeId)
  }

  test("places are fetched again from db when cached records expire") { f =>

    val placeType = PlaceType.RESTAURANT
    val placesDBRetriever = new PlacesDBRetriever(f.db, config.cacheConfig.copy(timeToLive = 5 seconds))

    val latLngRegion = LatLngRegion(45, 25)
      val locationScanResult = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType, narrowRadiusIfReturnLimitReached = false)
    f.db.insertPlaces(locationScanResult, latLngRegion, placeType).futureValue

    val resultsFromDB = f.db.getPlacesForLatLngRegion(latLngRegion, placeType).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(latLngRegion, placeType).futureValue

    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.db.dropPlacesTable.futureValue
    Thread.sleep(5000)
    recoverToSucceededIf[GenericDatabaseException] {
      placesDBRetriever.getPlaces(latLngRegion, placeType)
    }
  }
}
