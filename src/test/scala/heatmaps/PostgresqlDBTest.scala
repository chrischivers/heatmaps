package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{PlaceTableSchema, PostgresqlDB}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.scalatest.fixture

class PostgresqlDBTest extends fixture.FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

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

  test("heatmaps.db.Place results persisted to DB should match those retrieved") { f =>

    val placeType1 = PlaceType.RESTAURANT
    val placeType2 = PlaceType.LODGING

    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds, DefaultView(new LatLng(0,0),0))
    val locationScanResult1 = f.locationScanner.scanForPlacesInCity(city, 500, placeType1, narrowRadiusIfReturnLimitReached = false)
    f.db.insertPlaces(locationScanResult1, city, placeType1).futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInCity(city, 500, placeType2, narrowRadiusIfReturnLimitReached = false)
    f.db.insertPlaces(locationScanResult2, city, placeType2).futureValue

    val resultsFromDB1 = f.db.getPlacesForCity(city, placeType1).futureValue
    resultsFromDB1.size should be > 0
    resultsFromDB1.map(_.placeId) == locationScanResult1.map(_.placeId)

    val resultsFromDB2 = f.db.getPlacesForCity(city, placeType1).futureValue
    resultsFromDB2.size should be > 0
    resultsFromDB2.map(_.placeId) == locationScanResult2.map(_.placeId)
  }
}
