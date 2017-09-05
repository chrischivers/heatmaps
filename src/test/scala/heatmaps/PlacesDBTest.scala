package heatmaps

import com.google.maps.model.{PlaceType => GooglePlaceType}
import heatmaps.config.{ConfigLoader, Definitions}
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.LatLngRegion
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

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
    val placesDBRetriever = new PlacesRetriever(placesTable, config.cacheConfig, config.mapsConfig)
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

  test("Place results persisted to DB should match those retrieved") { f =>

    val placeCategory1 = Definitions.categories.find(_.id == "RESTAURANT").get
    val placeCategory2 = Definitions.categories.find(_.id == "LODGING").get

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult1 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeCategory1).futureValue
    f.placesTable.insertPlaces(locationScanResult1, latLngRegion, placeCategory1).futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeCategory2).futureValue
    f.placesTable.insertPlaces(locationScanResult2, latLngRegion, placeCategory2).futureValue

    val resultsFromDB1 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory1).futureValue
    resultsFromDB1.size should be > 0
    resultsFromDB1.map(_.placeId) should contain allElementsOf locationScanResult1.map(_.placeId)
    resultsFromDB1.map(_.latLngRegion).toSet shouldBe Set(latLngRegion)

    val resultsFromDB2 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory2).futureValue
    resultsFromDB2.size should be > 0
    resultsFromDB2.map(_.placeId) should contain allElementsOf locationScanResult2.map(_.placeId)
    resultsFromDB2.map(_.latLngRegion).toSet shouldBe Set(latLngRegion)
  }



  test("Place results persisted separately for two regions should all be obtained using bulk get") { f =>

    val placeCategory = Definitions.categories.find(_.id == "RESTAURANT").get

    val latLngRegion1 = LatLngRegion(45, 25)
    val latLngRegion2 = LatLngRegion(46, 25)
    val locationScanResult1 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion1, 10000, placeCategory).futureValue
    f.placesTable.insertPlaces(locationScanResult1, latLngRegion1, placeCategory).futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion2, 10000, placeCategory).futureValue
    f.placesTable.insertPlaces(locationScanResult2, latLngRegion2, placeCategory).futureValue

    val resultsFromDB = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion1, latLngRegion2), placeCategory).futureValue
    resultsFromDB.size shouldBe locationScanResult1.size + locationScanResult2.size
    resultsFromDB.map(_.placeId) should contain allElementsOf locationScanResult1.map(_.placeId) ++ locationScanResult2.map(_.placeId)

  }

  test("Place results persisted to DB should correspond to correct zoom level ") { f =>
    val schema = f.placesTable.schema
    val placeIdsAndMinZooms = List(("Id1", 5), ("Id2", 10), ("Id3", 15))
    val latLngRegion = LatLngRegion(3,101)
    val placeCategory = Definitions.categories.find(_.id == "RESTAURANT").get
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.category}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.minZoomLevel}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,?,'now');
         |
      """.stripMargin

    Future.sequence(placeIdsAndMinZooms.map{ case(placeId, minZoom) =>
      f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(placeId, placeCategory.id, latLngRegion.toString, latLngRegion.lat, latLngRegion.lng, minZoom))
    }).futureValue

   val regionsAtZoom1 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = Some(1)).futureValue
    regionsAtZoom1 should have size 0

    val regionsAtZoom5 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = Some(5)).futureValue
    regionsAtZoom5 should have size 1

    val regionsAtZoom9 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = Some(9)).futureValue
    regionsAtZoom9 should have size 1

    val regionsAtZoom10 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = Some(10)).futureValue
    regionsAtZoom10 should have size 2

    val regionsAtZoom15 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = Some(15)).futureValue
    regionsAtZoom15 should have size 3

    val regionsAtZoom16 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = Some(16)).futureValue
    regionsAtZoom16 should have size 3

    val regionsWithNoZoomSpecified = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeCategory, zoom = None).futureValue
    regionsWithNoZoomSpecified should have size 3
  }


  test("A record in places table can be updated with a name and a company added") { f =>
    val schema = f.placesTable.schema
    val mcDonaldsPlaceId = "ChIJ4VsFx-FFzDERn7dTZs447D4"
    val latLngRegion = LatLngRegion(3,101)
    val restaurant = Definitions.categories.find(_.id == "RESTAURANT").get
    val mcdonalds = Definitions.companies.find(_.id == "MCDONALDS").get
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.category}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(mcDonaldsPlaceId, restaurant.id, latLngRegion.toString, latLngRegion.lat, latLngRegion.lng)).futureValue

    val regionsContainingNullNames = f.placesTable.getLatLngRegionsContainingNullPlaceNames(restaurant).futureValue
    regionsContainingNullNames.get should have size 1
    regionsContainingNullNames.get.head shouldBe latLngRegion

    f.placesTable.countPlacesForLatLngRegion(latLngRegion, restaurant).futureValue shouldBe 1
    val zoomLevel = config.mapsConfig.minZoom + Random.nextInt((config.mapsConfig.maxZoom - config.mapsConfig.minZoom) + 1)
    f.placesTable.updatePlace(mcDonaldsPlaceId, restaurant, "McDonald's Test", zoomLevel).futureValue
    val resultsWithoutSubType = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), restaurant).futureValue

    resultsWithoutSubType should have size 1
    resultsWithoutSubType.head.placeId shouldBe mcDonaldsPlaceId
    resultsWithoutSubType.head.placeName.get shouldBe "McDonald's Test"
    resultsWithoutSubType.head.company.isDefined shouldBe false

    f.placesTable.updateCompany(mcdonalds).futureValue

    val resultsWithSubType = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), restaurant).futureValue
    resultsWithSubType should have size 1
    resultsWithSubType.head.placeId shouldBe mcDonaldsPlaceId
    resultsWithSubType.head.placeName.get shouldBe "McDonald's Test"
    resultsWithSubType.head.company.get shouldBe mcdonalds.id
    resultsWithSubType.head.zoom.get shouldBe zoomLevel
  }
}

