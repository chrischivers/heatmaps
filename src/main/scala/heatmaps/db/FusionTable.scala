package heatmaps.db

import java.io.{File, InputStreamReader}
import java.util.{Collections, Date}

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver
import com.google.api.client.googleapis.auth.oauth2.{GoogleAuthorizationCodeFlow, GoogleClientSecrets}
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.store.FileDataStoreFactory
import com.google.api.services.fusiontables.{Fusiontables, FusiontablesScopes}
import com.google.api.services.fusiontables.model.{Column, Sqlresponse, Table}
import com.google.maps.model.{LatLng, PlacesSearchResult}
import heatmaps.{City, FusionDBConfig}
import org.joda.time.DateTime

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class FusionTable(fusionDBConfig: FusionDBConfig, val schema: PlaceTableSchema, requestsPerMinuteThrottle: Double, recreateTableIfExists: Boolean = false) extends PlacesDatabase {

  private val DATA_STORE_DIR = new File(System.getProperty("user.home"), ".store/heatmaps")
  println(DATA_STORE_DIR.getAbsolutePath)
  private val JSON_FACTORY = JacksonFactory.getDefaultInstance
  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR)

  private val credential: Credential = { // load client secrets
    val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(getClass.getResourceAsStream("/" + fusionDBConfig.clientSecretsFileName)))
    val flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets, Collections.singleton(FusiontablesScopes.FUSIONTABLES)).setDataStoreFactory(dataStoreFactory).build
    new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver.Builder().setPort(58008).build()).authorize("user")
  }
  val fusiontables: Fusiontables = new Fusiontables.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("heatmaps").build
  val placesTable: Table = getPlacesTable(recreateTableIfExists)

  logger.info(s"PlacesTable set as ${placesTable.getTableId}")

  private def getPlacesTable(createNewIfExisting: Boolean = false): Table = {

    logger.info(s"Getting places table. CreateNewIfExisting set to $createNewIfExisting")

    val existingTables = fusiontables.table().list().execute().getItems.asScala.toList
    logger.info(s"The following fusion tables exist ${existingTables.map(table => table.getName + "(" + table.getTableId + ")")}")
    val existingMatchingTables = existingTables.filter(table => table.getName == schema.tableName)
    logger.info(s"The following fusion tables exist matching schema table table ${existingMatchingTables.map(table => table.getName + "(" + table.getTableId + ")")}")
    if (createNewIfExisting) {
      existingMatchingTables.foreach(table => dropPlacesTable(table.getTableId))
      createNewTable
    } else {
      existingMatchingTables match {
        case Nil => createNewTable
        case head :: Nil => head
        case head :: rest =>
          rest.foreach(table => dropPlacesTable(table.getTableId))
          head
      }
    }
  }

  private def createNewTable: Table = {
    val table = new Table()
    table.setName(schema.tableName)
    table.setIsExportable(true)
    table.setColumns(List(
      new Column().setName(schema.placeId).setType("STRING"),
      new Column().setName(schema.placeType).setType("STRING"),
      new Column().setName(schema.placeName).setType("STRING"),
      new Column().setName(schema.cityName).setType("STRING"),
      new Column().setName("location").setType("LOCATION"),
      new Column().setName(schema.lastUpdated).setType("DATETIME")
    ).asJava)
    val createdTable = fusiontables.table.insert(table).execute()
    logger.info(s"Table created (created ID ${createdTable.getTableId})")
    createdTable
  }

  override def insertPlaces(placeSearchResults: List[PlacesSearchResult], city: City, placeType: String): Future[Unit] = {

      getPlacesForCity(city).map { existingPlaces =>
        logger.info(s"Found ${existingPlaces.size} existing places in table ${placesTable.getTableId}")
        val newPlaceSearchResults = placeSearchResults.filter(result => !existingPlaces.exists(place => place.placeId == result.placeId))
        logger.info(s"${newPlaceSearchResults.size} new records to insert. Ignoring ${placeSearchResults.size - newPlaceSearchResults.size} as already exist")
        logger.info(s"Starting insert of ${newPlaceSearchResults.size} places into DB (table ${placesTable.getTableId})")
        newPlaceSearchResults.foldLeft(1)((acc, result) => {
          logger.info(s"Inserting place $acc of ${newPlaceSearchResults.size} into DB (table ${placesTable.getTableId})")
          retry(fusionDBConfig.numberRetries)(insertPlace(placesTable.getTableId, result, city, placeType))
          acc + 1
        })
      }
  }

  private def insertPlace(placesTableID: String, placeSearchResult: PlacesSearchResult, city: City, placeType: String) = {
    val statement =
      s"""
         |
         |INSERT INTO $placesTableID (${schema.placeId}, ${schema.placeType}, ${schema.cityName}, location, ${schema.lastUpdated})
         |    VALUES ('${placeSearchResult.placeId}','$placeType','${city.name}','${placeSearchResult.geometry.location.lat},${placeSearchResult.geometry.location.lng}','${new DateTime(new Date())}');
         |
      """.stripMargin
    val startTime = System.currentTimeMillis()
    try {
      val sqlResponse = fusiontables.query.sql(statement).execute()
      println(sqlResponse.toPrettyString)
      credential.refreshToken()
    } catch {
      case ex:Exception => logger.info("Exception thrown by sql query execute.")
        throw ex
    }

    Thread.sleep(calculateSleepTime)

    def calculateSleepTime = {
      val sleepTime = ((60.0 / requestsPerMinuteThrottle) * 1000).toLong - (System.currentTimeMillis() - startTime)
      if (sleepTime < 0) 0L else sleepTime
    }
  }


  override def getPlacesForCity(city: City): Future[List[Place]] = {
    logger.info(s"getting places for city: $city")
    val query = s"SELECT * FROM ${placesTable.getTableId} WHERE ${schema.cityName} = '${city.name}'"
    val sqlResponse: Sqlresponse = fusiontables.query().sql(query).execute()
    val columns = sqlResponse.getColumns.asScala.toList
    val rowsOpt = Option(sqlResponse.getRows)
    Future(
      rowsOpt match {
        case None => List.empty
        case Some(rows) => rows.asScala.toList.map(row => {
          val fields = row.asScala.toList
          val latLng = fields(columns.indexOf("location")).asInstanceOf[String].split(",")
          Place(
            placeId = fields(columns.indexOf(schema.placeId)).asInstanceOf[String],
            placeType = fields(columns.indexOf(schema.placeType)).asInstanceOf[String],
            latLng = new LatLng(latLng(0).toDouble, latLng(1).toDouble)
          )
        })
      })
  }

  private def dropPlacesTable(tableId: String): Future[Unit] = {
    logger.info(s"Dropping Places Table ID $tableId")
    fusiontables.table.delete(tableId).execute()
    Future(())
  }

  @annotation.tailrec
  // Code borrowed from http://stackoverflow.com/questions/7930814/whats-the-scala-way-to-implement-a-retry-able-call-like-this-one
  private def retry[T](n: Int)(fn: => T): T = {
      util.Try {
      fn
    } match {
      case util.Success(x) => x
      case _ if n > 1 => {
        logger.info(s"Retrying operation. Attempt ${(fusionDBConfig.numberRetries - n) + 1}")
        retry(n - 1)(fn)
      }
      case util.Failure(e) => throw e
    }
  }
}

