package heatmaps.db

import com.github.mauricio.async.db.{Connection, QueryResult}
import com.github.mauricio.async.db.postgresql.PostgreSQLConnection
import com.google.maps.model.{LatLng, PlaceType, PlacesSearchResult}
import com.typesafe.scalalogging.StrictLogging
import heatmaps.models.{LatLngRegion, Place}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

trait Table[T <: Connection] extends StrictLogging {
  val db: DB[T]
  val schema: Schema

  protected def createTable: Future[QueryResult]

  def dropTable(implicit executor: ExecutionContext): Future[Unit] = {
    logger.info(s"Dropping ${schema.tableName}")
    val query = s"DROP TABLE IF EXISTS ${schema.tableName}"
    db.connectionPool.sendQuery(query).map(_ => ())
  }
}

class PlacesTable(val db: DB[PostgreSQLConnection], val schema: PlaceTableSchema, createNewTable: Boolean = false)(implicit ec: ExecutionContext) extends Table[PostgreSQLConnection] {

  if (createNewTable) {
    Await.result({
      logger.info(s"Creating new table ${schema.tableName}")
      for {
        _ <- dropTable
        newTable <- createTable
      } yield newTable
    }, 1 minute) //Blocks while table created

  }

  override protected def createTable: Future[QueryResult] = {
    logger.info(s"Creating Table ${schema.tableName}")
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendQuery(
        s"""
           |CREATE TABLE IF NOT EXISTS
           |${schema.tableName} (
           |    ${schema.placeId} varchar NOT NULL,
           |    ${schema.placeType} varchar NOT NULL,
           |    ${schema.placeName} varchar,
           |    ${schema.latLngRegion} varchar,
           |    ${schema.lat} real NOT NULL,
           |    ${schema.lng} real NOT NULL,
           |    ${schema.lastUpdated} timestamp NOT NULL,
           |    PRIMARY KEY(${schema.primaryKey.mkString(",")})
           |);
        """.stripMargin)
    } yield queryResult
  }

  def insertPlaces(placeSearchResults: List[PlacesSearchResult], latLngRegion: LatLngRegion, placeType: PlaceType): Future[Unit] = {
    logger.info(s"Inserting ${placeSearchResults.size} places into DB for latLngRegion $latLngRegion and type $placeType ")
    for {
      _ <- db.connectToDB
      result <- Future.sequence(placeSearchResults.zipWithIndex.map{ case (result, index)  => {
          logger.info(s"Inserting place $index of ${placeSearchResults.size} into DB")
          insertPlace(result, latLngRegion, placeType.name())
        }
      })
    } yield result.map(_ => ())
  }

  private def insertPlace(placeSearchResult: PlacesSearchResult, latLngRegion: LatLngRegion, placeType: String): Future[QueryResult] = {
    val statement =
      s"""
         |
        |INSERT INTO ${schema.tableName} (${schema.placeId}, ${schema.placeType}, ${schema.latLngRegion}, ${schema.lat}, ${schema.lng}, ${schema.lastUpdated})
         |    VALUES (?,?,?,?,?,'now');
         |
      """.stripMargin

    db.connectionPool.sendPreparedStatement(statement, List(placeSearchResult.placeId, placeType, latLngRegion.toString, placeSearchResult.geometry.location.lat, placeSearchResult.geometry.location.lng))
  }

  def getPlacesForLatLngRegion(latLngRegion: LatLngRegion, placeType: PlaceType): Future[List[Place]] = {
    logger.info(s"getting places for latLngRegion $latLngRegion from DB")
    val query = s"SELECT * " +
      s"FROM ${schema.tableName} " +
      s"WHERE ${schema.latLngRegion} = ? " +
      s"AND ${schema.placeType} = ?"
    for {
      _ <- db.connectToDB
      queryResult <- db.connectionPool.sendPreparedStatement(query, List(latLngRegion.toString, placeType.name()))
    } yield {
      queryResult.rows match {
        case Some(resultSet) => resultSet.map(res => {
          Place(
            res.apply(schema.placeId).asInstanceOf[String],
            res.apply(schema.placeType).asInstanceOf[String],
            new LatLng(res.apply(schema.lat).asInstanceOf[Float].toDouble, res.apply(schema.lng).asInstanceOf[Float].toDouble)
          )
        }).toList
        case None => List.empty
      }
    }
  }
}