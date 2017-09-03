package heatmaps

import com.github.mauricio.async.db.postgresql.exceptions.GenericDatabaseException
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db.{PlaceTableSchema, PlacesTable, PostgresDB}
import heatmaps.models.Category.Restaurant
import heatmaps.models.Company.McDonalds
import heatmaps.models.LatLngRegion
import heatmaps.scanner.{LocationScanner, PlacesApiRetriever}
import heatmaps.web.PlacesRetriever
import org.scalatest.Matchers._
import org.scalatest.RecoverMethods._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.fixture

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Random

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

  test("places fetched from cache are same as those fetched directly from DB") { f =>

    val category = Restaurant
    val placesDBRetriever = new PlacesRetriever(f.placesTable, config.cacheConfig, config.mapsConfig)

    val latLngRegion = LatLngRegion(45, 25)
    (for {
      locationScanResult <- f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, category)
      _ <- f.placesTable.insertPlaces(locationScanResult, latLngRegion, category)
    } yield ()).futureValue

    val resultsFromDB = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), category, zoom = Some(3)).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(List(latLngRegion), category, zoomOpt = Some(3)).futureValue

    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.placesTable.dropTable.futureValue
    f.placesTable.createTable.futureValue
    val resultsFromCacheAgain = placesDBRetriever.getPlaces(List(latLngRegion), category, zoomOpt = Some(3)).futureValue
    resultsFromCacheAgain.map(_.placeId) should contain theSameElementsAs resultsFromDB.map(_.placeId)
  }

  test("places are fetched again from db when cached records expire") { f =>

    val category = Restaurant
    val placesDBRetriever = new PlacesRetriever(f.placesTable, config.cacheConfig.copy(timeToLive = 5 seconds), config.mapsConfig)

    val latLngRegion = LatLngRegion(45, 25)
    (for {
      locationScanResult <- f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion, 10000, category)
      _ <- f.placesTable.insertPlaces(locationScanResult, latLngRegion, category)
    } yield ()).futureValue

    val resultsFromDB = f.placesTable.getPlacesForLatLngRegions(List(latLngRegion), category).futureValue
    val resultsFromCache = placesDBRetriever.getPlaces(List(latLngRegion), category).futureValue

    resultsFromDB.size should be > 0
    resultsFromDB.map(_.placeId) should contain theSameElementsAs resultsFromCache.map(_.placeId)
    f.placesTable.dropTable.futureValue
    f.placesTable.createTable.futureValue
    Thread.sleep(5000)
    recoverToSucceededIf[GenericDatabaseException] {
      placesDBRetriever.getPlaces(List(latLngRegion), category)
    }
  }

  test("get requests for multiple LatLng regions are fetched both from cache and DB combined") { f =>

    val category = Restaurant
    val placesDBRetriever = new PlacesRetriever(f.placesTable, config.cacheConfig, config.mapsConfig)

    val latLngRegion1 = LatLngRegion(45, 25)
    val latLngRegion2 = LatLngRegion(46, 25)
    val locationScanResult1 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion1, 10000, category).futureValue
    f.placesTable.insertPlaces(locationScanResult1, latLngRegion1, category).futureValue

    val query = s"UPDATE ${f.placesTable.schema.tableName} SET ${f.placesTable.schema.minZoomLevel} = floor(random()*(18-2+1))+2;"
    f.placesTable.db.connectionPool.sendPreparedStatement(query).futureValue

    placesDBRetriever.getPlaces(List(latLngRegion1), category, zoomOpt = Some(18)).futureValue

    f.placesTable.dropTable.futureValue
    f.placesTable.createTable.futureValue

    val locationScanResult2 = f.locationScanner.scanForPlacesInLatLngRegion(latLngRegion2, 10000, category).futureValue
    f.placesTable.insertPlaces(locationScanResult2, latLngRegion2, category).futureValue
    f.placesTable.db.connectionPool.sendPreparedStatement(query).futureValue

    val results = placesDBRetriever.getPlaces(List(latLngRegion1, latLngRegion2), category, zoomOpt = Some(18)).futureValue
    results.size shouldBe locationScanResult1.size + locationScanResult2.size
    results.map(_.placeId) should contain allElementsOf locationScanResult1.map(_.placeId) ++ locationScanResult2.map(_.placeId)

  }

  test("places fetched from cache are filtered by place subtype if provided") { f =>

    val schema = f.placesTable.schema
    val placeIdsAndNames = (1 to 42).map(i => (s"Id$i", Random.nextString(10)))
    val latLngRegion = LatLngRegion(3,101)
    val category = Restaurant
    val company = McDonalds
    val placesDBRetriever = new PlacesRetriever(f.placesTable, config.cacheConfig, config.mapsConfig)
    val zoom = 2

    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.category}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.placeName}, ${schema.minZoomLevel}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,?,?,'now');
         |
      """.stripMargin

    Future.sequence(placeIdsAndNames.map{ case(placeId, name) =>
      f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(placeId, category.name, latLngRegion.toString, latLngRegion.lat, latLngRegion.lng,
        if(placeId == "Id1") "McDonald's" else name, zoom))
    }).futureValue

    f.placesTable.updateCompany(company).futureValue

    val fromDb = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(zoom)).futureValue
    fromDb should have size 42

    f.placesTable.dropTable.futureValue
    f.placesTable.createTable.futureValue

    val fromCache = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(zoom)).futureValue
    fromCache should have size 42

    val fromCacheWithSubType = placesDBRetriever.getPlaces(List(latLngRegion), McDonalds, None, Some(zoom)).futureValue
    fromCacheWithSubType should have size 1
  }

  test("places fetched from cache correspond to zoom level (correct cache logic)") { f =>

    val schema = f.placesTable.schema
    val placeIdsWithIndex = (1 to 42).map(i => s"Id$i").zipWithIndex
    val latLngRegion = LatLngRegion(3,101)
    val category = Restaurant
    val placesDBRetriever = new PlacesRetriever(f.placesTable, config.cacheConfig, config.mapsConfig)

    val statement =
      s"""
         |
         |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.category}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.minZoomLevel}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,?,'now');
         |
      """.stripMargin

    Future.sequence(placeIdsWithIndex.map{ case(placeId, index) =>
      f.placesTable.db.connectionPool.sendPreparedStatement(statement, List(placeId, category.name, latLngRegion.toString, latLngRegion.lat, latLngRegion.lng,
        if(index % 2 == 0) 2 else 3))
    }).futureValue

    val fromDb1 = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(2)).futureValue
    fromDb1 should have size 21

    val fromDb2 = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(3)).futureValue
    fromDb2 should have size 42

    val fromDb3 = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(4)).futureValue
    fromDb3 should have size 42

    val fromDBWithNoneZoom = placesDBRetriever.getPlaces(List(latLngRegion), category, None, None).futureValue
    fromDBWithNoneZoom should have size 42

    f.placesTable.dropTable.futureValue
    f.placesTable.createTable.futureValue

    val fromCache1 = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(2)).futureValue
    fromCache1 should have size 21

    val fromCache2 = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(3)).futureValue
    fromCache2 should have size 42

    // Result set will be empty as cache does not hold enough information to get all zoom range
    val fromCache3 = placesDBRetriever.getPlaces(List(latLngRegion), category, None, Some(4)).futureValue
    fromCache3 should have size 0

    val fromCacheWithNoneZoom = placesDBRetriever.getPlaces(List(latLngRegion), category, None, None).futureValue
    fromCacheWithNoneZoom should have size 0
  }
}
