package heatmaps

import com.google.maps.model.{LatLng, PlaceType}
import heatmaps.db.{PlaceTableSchema, PostgresqlDB}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import scala.concurrent.ExecutionContext.Implicits.global


class LocationScannerTest extends FunSuite {

  val config = ConfigLoader.defaultConfig //TODO configs for test db

  test("Location Scanner should retrieve a list of all places in a given latLngRegion") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(tableName = "placestest"), recreateTableIfExists = true)
    val placesDBRetriever = new PlacesDBRetriever(db, config.cacheConfig)

    val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)
    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult = ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT)

    locationScanResult.size should be > 500
    locationScanResult.size should be < 1000
  }

  test("place retriever results for mid point of a given area should be a subset of location scanner results for the entire area") {
    val placesApiRetriever = new PlacesApiRetriever(config)
    val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(tableName = "placestest"), recreateTableIfExists = true)
    val placesDBRetriever = new PlacesDBRetriever(db, config.cacheConfig)

    val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)
    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult = ls.scanForPlacesInLatLngRegion(latLngRegion, 10000, PlaceType.RESTAURANT)

    val midPointResults = placesApiRetriever.getPlaces(new LatLng(45.5, 25.5), 1000, PlaceType.RESTAURANT, narrowRadiusIfReturnLimitReached = true)

    midPointResults.size should be < locationScanResult.size

    val locationScanResultPlaceIds = locationScanResult.map(_.placeId)

    midPointResults.length should be < locationScanResultPlaceIds.length
    midPointResults.map(_.placeId).foreach(placeId => {
      locationScanResultPlaceIds should contain(placeId)
    })
  }

  test("Location scan with wide radius should return same result set as location scan with small radius") {

    val southWest = new LatLng(51.510640, -0.140641)
    val northEast = new LatLng(51.516890, -0.131972)

    val latLngRegion = LatLngRegion(45, 25)

    val placesApiRetriever = new PlacesApiRetriever(config)
    val db = new PostgresqlDB(config.postgresDBConfig, PlaceTableSchema(tableName = "placestest"), recreateTableIfExists = true)
    val placesDBRetriever = new PlacesDBRetriever(db, config.cacheConfig)
    val ls = new LocationScanner(placesApiRetriever, placesDBRetriever)

    val scanResultsLargeRadius = ls.scanForPlacesInLatLngRegion(latLngRegion, 10000,  PlaceType.RESTAURANT)
    val scanResultsLargeRadiusPlaceIds = scanResultsLargeRadius.map(result => result.placeId)

    val scanResultsSmallRadius = ls.scanForPlacesInLatLngRegion(latLngRegion, 50000,  PlaceType.RESTAURANT)
    val scanResultsSmallRadiusPlaceIds = scanResultsSmallRadius.map(result => result.placeId)

    scanResultsSmallRadiusPlaceIds.foreach(placeId => scanResultsLargeRadiusPlaceIds should contain (placeId))
  }
}
