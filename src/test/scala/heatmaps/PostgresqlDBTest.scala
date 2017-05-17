package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{PlaceTableSchema, PostgresqlDB}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PostgresqlDBTest extends FunSuite with ScalaFutures {

  val config = ConfigLoader.defaultConfig

  override implicit val patienceConfig = PatienceConfig(
    timeout = scaled(5 minutes),
    interval = scaled(500 millis)
  )

  test("heatmaps.db.Place results persisted to DB should match those retrieved") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val ls = new LocationScanner(placesApiRetriever)
    val db = new PostgresqlDB(config.dBConfig, PlaceTableSchema(tableName = "testPlaces"))
    db.dropPlacesTable.futureValue

    val latLngBounds = LatLngBounds(new LatLng(51.509482, -0.138981), new LatLng(51.516319, -0.131428))
    val city = City("TestCity", latLngBounds)
    val locationScanResult = ls.scanCity(city, 500, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = false)
    db.insertPlaces(locationScanResult, city, PlaceType.RESTAURANT.name()).futureValue

    val resultsFromDB = db.getPlacesForCity(city).futureValue
    resultsFromDB.size should be > 0
    resultsFromDB.map(_.place_id) == locationScanResult.map(_.placeId)

  }

}
