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
import com.google.api.services.fusiontables.model.{Column, Table}
import com.google.maps.model.PlacesSearchResult
import heatmaps.City
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future


class FusionTable(val schema: PlaceTableSchema) extends PlacesDatabase {

  private val DATA_STORE_DIR = new File(System.getProperty("user.home"), ".store/fusion_tables_sample")
  private val JSON_FACTORY = JacksonFactory.getDefaultInstance
  private val httpTransport = GoogleNetHttpTransport.newTrustedTransport
  private val dataStoreFactory = new FileDataStoreFactory(DATA_STORE_DIR)

  private val credential: Credential = { // load client secrets
    val clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(getClass.getResourceAsStream("/client_secrets.json")))
    val flow = new GoogleAuthorizationCodeFlow.Builder(httpTransport, JSON_FACTORY, clientSecrets, Collections.singleton(FusiontablesScopes.FUSIONTABLES)).setDataStoreFactory(dataStoreFactory).build
    new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver.Builder().setPort(58008).build()).authorize("user")
  }

  val fusiontables: Fusiontables = new Fusiontables.Builder(httpTransport, JSON_FACTORY, credential).setApplicationName("heatmaps").build

  private def getPlacesTable: Table = {

    val existingTables = fusiontables.table().list().execute().getItems.asScala
    logger.info(s"The following fusion tables already exist $existingTables")
    val tableOpt = fusiontables.table().list().execute().getItems.asScala.find(table => table.getName == schema.tableName)

    tableOpt.getOrElse {

      val table = new Table()
      table.setName(schema.tableName)
      table.setIsExportable(false)

      table.setColumns(List(
        new Column().setName(schema.placeId).setType("STRING"),
        new Column().setName(schema.placeType).setType("STRING"),
        new Column().setName(schema.placeName).setType("STRING"),
        new Column().setName(schema.cityName).setType("STRING"),
        new Column().setName("location").setType("LOCATION"),
        new Column().setName(schema.lastUpdated).setType("DATETIME")
      ).asJava)
      fusiontables.table.insert(table).execute()
    }
  }

  override def insertPlaces(placeSearchResults: List[PlacesSearchResult], city: City, placeType: String): Future[Unit] = {
    val placesTable = getPlacesTable
    placeSearchResults.foreach(result => insertPlace(placesTable.getTableId, result, city, placeType))
    Future(())
  }

  private def insertPlace(placesTableID: String, placeSearchResult: PlacesSearchResult, city: City, placeType: String) = {
    val statement =
      s"""
         |
         |INSERT INTO $placesTableID (${schema.placeId}, ${schema.placeType}, ${schema.cityName}, location, ${schema.lastUpdated})
         |    VALUES ('${placeSearchResult.placeId}','$placeType','${city.name}','${placeSearchResult.geometry.location.lat} ${placeSearchResult.geometry.location.lng}','${new DateTime(new Date())}');
         |
      """.stripMargin


    logger.info("executing statement")
    val sqlResponse = fusiontables.query.sql(statement).execute()
    println(sqlResponse.toPrettyString)
  }

  override def getPlacesForCity(city: City): Future[List[Place]] = {
    logger.info(s"getting places for city: $city")
    val placesTable = getPlacesTable
    val query = s"SELECT * FROM ${placesTable.getTableId} WHERE ${schema.cityName} = ${city.name}"
    val sqlResponse = fusiontables.query().sql(query).execute()
    println(sqlResponse.getRows)
    Future(List())
//
//    for {
//      _ <- connectToDB
//      queryResult <- connectionPool.sendPreparedStatement(query, List(city.name))
//    // _ <- disconnectFromDB
//    } yield {
//      queryResult.rows match {
//        case Some(resultSet) => resultSet.map(res => {
//          heatmaps.db.Place(
//            res.apply(schema.placeId).asInstanceOf[String],
//            res.apply(schema.placeType).asInstanceOf[String],
//            new LatLng(res.apply(schema.lat).asInstanceOf[Float].toDouble, res.apply(schema.lng).asInstanceOf[Float].toDouble)
//          )
//        }).toList
//        case None => List.empty
//      }
//    }
  }

  override def dropPlacesTable: Future[Unit] = {
    val placesTable = getPlacesTable
    fusiontables.table.delete(placesTable.getTableId)
    Future(())
  }
}

