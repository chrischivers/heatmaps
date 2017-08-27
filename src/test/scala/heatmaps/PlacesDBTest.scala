package heatmaps

import com.google.maps.model.PlaceType
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.{LatLngRegion, McDonalds}
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever
import org.scalatest.Matchers._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
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

  test("Place results persisted to DB should match those retrieved") { f =>

    val placeType1 = PlaceType.RESTAURANT
    val placeType2 = PlaceType.LODGING

    val latLngRegion = LatLngRegion(45, 25)
    val locationScanResult1 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType1).futureValue
    f.placesTable.insertPlaces(locationScanResult1, latLngRegion, placeType1).futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, placeType2).futureValue
    f.placesTable.insertPlaces(locationScanResult2, latLngRegion, placeType2).futureValue

    val resultsFromDB1 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType1).futureValue
    resultsFromDB1.size should be > 0
    resultsFromDB1.map(_.placeId) should contain allElementsOf locationScanResult1.map(_.placeId)
    resultsFromDB1.map(_.latLngRegion).toSet shouldBe Set(latLngRegion)

    val resultsFromDB2 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType2).futureValue
    resultsFromDB2.size should be > 0
    resultsFromDB2.map(_.placeId) should contain allElementsOf locationScanResult2.map(_.placeId)
    resultsFromDB2.map(_.latLngRegion).toSet shouldBe Set(latLngRegion)
  }



  test("Place results persisted separately for two regions should all be obtained using bulk get") { f =>

    val placeType = PlaceType.RESTAURANT

    val latLngRegion1 = LatLngRegion(45, 25)
    val latLngRegion2 = LatLngRegion(46, 25)
    val locationScanResult1 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion1, 10000, placeType).futureValue
    f.placesTable.insertPlaces(locationScanResult1, latLngRegion1, placeType).futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion2, 10000, placeType).futureValue
    f.placesTable.insertPlaces(locationScanResult2, latLngRegion2, placeType).futureValue

    val resultsFromDB = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion1, latLngRegion2), placeType).futureValue
    resultsFromDB.size shouldBe locationScanResult1.size + locationScanResult2.size
    resultsFromDB.map(_.placeId) should contain allElementsOf locationScanResult1.map(_.placeId) ++ locationScanResult2.map(_.placeId)

  }

  test("Place results persisted to DB should correspond to correct zoom level ") { f =>
    val schema = f.placesTable.schema
    val placeIdsAndMinZooms = List(("Id1", 5), ("Id2", 10), ("Id3", 15))
    val latLngRegion = LatLngRegion(3,101)
    val placeType = PlaceType.RESTAURANT
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.placeType}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.minZoomLevel}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,?,'now');
         |
      """.stripMargin

    Future.sequence(placeIdsAndMinZooms.map{ case(placeId, minZoom) =>
      f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(placeId, PlaceType.RESTAURANT.name(), latLngRegion.toString, latLngRegion.lat, latLngRegion.lng, minZoom))
    }).futureValue

   val regionsAtZoom1 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = Some(1)).futureValue
    regionsAtZoom1 should have size 0

    val regionsAtZoom5 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = Some(5)).futureValue
    regionsAtZoom5 should have size 1

    val regionsAtZoom9 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = Some(9)).futureValue
    regionsAtZoom9 should have size 1

    val regionsAtZoom10 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = Some(10)).futureValue
    regionsAtZoom10 should have size 2

    val regionsAtZoom15 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = Some(15)).futureValue
    regionsAtZoom15 should have size 3

    val regionsAtZoom16 = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = Some(16)).futureValue
    regionsAtZoom16 should have size 3

    val regionsWithNoZoomSpecified = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, zoom = None).futureValue
    regionsWithNoZoomSpecified should have size 3
  }


  test("Zoom result should update random selection of rows based on min and max possible zoom levels") { f =>
    val schema = f.placesTable.schema
    val placeIds = (1 to 42).map(i => s"Id$i")
    val latLngRegion = LatLngRegion(3,101)
    val placeType = PlaceType.RESTAURANT
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.placeType}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    Future.sequence(placeIds.map{ placeId =>
      f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(placeId, PlaceType.RESTAURANT.name(), latLngRegion.toString, latLngRegion.lat, latLngRegion.lng))
    }).futureValue.filter(queryResult => queryResult.rowsAffected == 0) should have size 0

    val zoomRange = 0 to 20
    Future.sequence(zoomRange.map { zoom =>
      f.placesTable.updateMinZooms(minZoomToSet = zoom, placeType, minPossibleZoom = 0, maxPossibleZoom = 20)
    }).futureValue

    zoomRange.foreach { zoom =>
      val results = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), placeType, None, Some(zoom)).futureValue
      results should have size ((zoom + 1) * 2)
    }

    Future.sequence(zoomRange.map { zoom =>
      f.placesTable.updateMinZooms(minZoomToSet = zoom, placeType, minPossibleZoom = 0, maxPossibleZoom = 20)
    }).futureValue.filter(queryResult => queryResult.rowsAffected > 0) should have size 0
  }

  test("A record in places table can be updated with a name and a subtype added") { f =>
    val schema = f.placesTable.schema
    val mcDonaldsPlaceId = "ChIJ4VsFx-FFzDERn7dTZs447D4"
    val latLngRegion = LatLngRegion(3,101)
    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.placeType}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(mcDonaldsPlaceId, PlaceType.RESTAURANT.name(), latLngRegion.toString, latLngRegion.lat, latLngRegion.lng)).futureValue

    val regionsContainingNullNames = f.placesTable.getLatLngRegionsContainingNullPlaceNames(PlaceType.RESTAURANT).futureValue
    regionsContainingNullNames should have size 1
    regionsContainingNullNames.head shouldBe latLngRegion

    f.placesTable.countPlacesForLatLngRegion(latLngRegion, PlaceType.RESTAURANT).futureValue shouldBe 1

    f.placesTable.updatePlace(mcDonaldsPlaceId, PlaceType.RESTAURANT.name(), "McDonald's Test").futureValue
    val resultsWithoutSubType = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), PlaceType.RESTAURANT).futureValue

    resultsWithoutSubType should have size 1
    resultsWithoutSubType.head.placeId shouldBe mcDonaldsPlaceId
    resultsWithoutSubType.head.placeName.get shouldBe "McDonald's Test"
    resultsWithoutSubType.head.placeSubType.isDefined shouldBe false

    f.placesTable.updateSubtypes(McDonalds).futureValue

    val resultsWithSubType = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), PlaceType.RESTAURANT).futureValue
    resultsWithSubType should have size 1
    resultsWithSubType.head.placeId shouldBe mcDonaldsPlaceId
    resultsWithSubType.head.placeName.get shouldBe "McDonald's Test"
    resultsWithSubType.head.placeSubType.get shouldBe McDonalds.name
  }
}

