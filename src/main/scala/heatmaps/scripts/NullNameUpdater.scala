package heatmaps.scripts

import com.github.mauricio.async.db.QueryResult
import com.google.maps.PlacesApi
import com.google.maps.model.PlaceType
import com.typesafe.scalalogging.StrictLogging
import heatmaps.config.ConfigLoader
import heatmaps.db._
import heatmaps.models.LatLngRegion
import heatmaps.scanner.PlacesApiRetriever

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.io.Source

object NullNameUpdater extends App with StrictLogging {
  val config = ConfigLoader.defaultConfig
  val db = new PostgresDB(config.dBConfig)
  val placesTable = new PlacesTable(db, PlaceTableSchema(), createNewTable = false) {
    def updatePlace(placeID: String, placeType: String, name: String): Future[QueryResult] = {
      val statement =
        s"""
           |
         |UPDATE ${schema.tableName}
           |SET ${schema.placeName} = ?
           |WHERE ${schema.placeId} = ?
           |AND ${schema.placeType} = ?
           |
      """.stripMargin

      db.connectionPool.sendPreparedStatement(statement, List(name, placeID, placeType))
    }
  }
  val placesApiRetriever = new PlacesApiRetriever(config) {
    def getPlaceNameFromApi(placeId: String): Future[String] = {
      Future(PlacesApi.placeDetails(context, placeId).await().name)
    }
  }

  val regionsWithNullPlaceNames =  Source.fromResource("regions_with_null_place_names")
    .getLines().map(line => LatLngRegion(line.split(",")(0).toInt, line.split(",")(2).toInt))

  regionsWithNullPlaceNames.foreach(region => {
  Await.result({placesTable.getPlacesForLatLngRegion(region, PlaceType.RESTAURANT).flatMap(places => {
    Future.sequence(places.filter(_.placeName.isEmpty).map(place => {
      placesApiRetriever.getPlaceNameFromApi(place.placeId).flatMap(name => {
        placesTable.updatePlace(place.placeId, place.placeType, name)
      })
    }))
  })}, 10 hours)
})
}