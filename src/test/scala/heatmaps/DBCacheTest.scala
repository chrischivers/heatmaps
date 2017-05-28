package heatmaps

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{FusionTable, PlaceTableSchema, PostgresqlDB}
import org.scalatest.FunSuite
import org.scalatest.RecoverMethods._
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.fixture

class DBCacheTest extends fixture.FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  case class FixtureParam(placesApiRetriever: PlacesApiRetriever, locationScanner: LocationScanner, db: PostgresqlDB)

  def withFixture(test: OneArgTest) = {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val locationScanner = new LocationScanner(placesApiRetriever)
    val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(), recreateTableIfExists = true)

    val testFixture = FixtureParam(placesApiRetriever, locationScanner, db)

    try {
      withFixture(test.toNoArgTest(testFixture))
    }
    finally {
      //fusionTable.dropPlacesTable
    }
  }

  test("places fetched from cache are same as those fetched directly from DB") { f =>

    val placesDBRetriever = new PlacesDBRetriever(f.db, config.cacheConfig)

    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds)
    val locationScanResult = f.locationScanner.scanCity(city, 500, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
    f.db.insertPlaces(locationScanResult, city, PlaceType.RESTAURANT.name()).futureValue

    val resultsFromDB = f.db.getPlacesForCity(city).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(city).futureValue

    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.db.dropPlacesTable.futureValue
    val resultsFromCacheAgain = placesDBRetriever.getPlaces(city).futureValue
    resultsFromCacheAgain.map(_.placeId) should contain theSameElementsAs resultsFromDB.map(_.placeId)
  }
  test("places fare fetched again from db when cached records expire") { f =>

    val placesDBRetriever = new PlacesDBRetriever(f.db, config.cacheConfig.copy(timeToLive = 5 seconds))

    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds)
    val locationScanResult = f.locationScanner.scanCity(city, 500, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
    f.db.insertPlaces(locationScanResult, city, PlaceType.RESTAURANT.name()).futureValue

    val resultsFromDB = f.db.getPlacesForCity(city).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(city).futureValue

    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.db.dropPlacesTable.futureValue
    Thread.sleep(5000)
    recoverToSucceededIf[GenericDatabaseException] {
      placesDBRetriever.getPlaces(city)
    }
  }
}
